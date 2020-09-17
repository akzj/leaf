// Copyright 2020-2026 The streamIO Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sstore

import (
	"github.com/akzj/streamIO/pkg/block-queue"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"path/filepath"
	"sort"
	"sync"
)

type committer struct {
	queue *block_queue.Queue

	maxMStreamTableSize int64
	mutableMStreamMap   *mStreamTable
	enpMap              *int64LockMap

	immutableMStreamMaps          []*mStreamTable
	maxImmutableMStreamTableCount int
	locker                        *sync.RWMutex

	flusher *flusher

	segments       map[string]*segment
	segmentsLocker *sync.RWMutex

	indexTable  *indexTable
	endWatchers *endWatchers
	manifest    *manifest

	blockSize int

	cbWorker      *cbWorker
	callbackQueue *block_queue.Queue
	sstore        *SStore
}

func newCommitter(options Options,
	endWatchers *endWatchers,
	indexTable *indexTable,
	sizeMap *int64LockMap,
	mutableMStreamMap *mStreamTable,
	queue *block_queue.Queue,
	manifest *manifest,
	blockSize int,
	sstore *SStore) *committer {

	cbQueue := block_queue.NewQueue(128)

	return &committer{
		queue:                         queue,
		maxMStreamTableSize:           options.MaxMStreamTableSize,
		mutableMStreamMap:             mutableMStreamMap,
		enpMap:                        sizeMap,
		immutableMStreamMaps:          make([]*mStreamTable, 0, 32),
		maxImmutableMStreamTableCount: options.MaxImmutableMStreamTableCount,
		locker:                        new(sync.RWMutex),
		flusher:                       newFlusher(manifest),
		segments:                      map[string]*segment{},
		segmentsLocker:                new(sync.RWMutex),
		indexTable:                    indexTable,
		endWatchers:                   endWatchers,
		manifest:                      manifest,
		blockSize:                     blockSize,
		cbWorker:                      newCbWorker(cbQueue),
		callbackQueue:                 cbQueue,
		sstore:                        sstore,
	}
}

func (c *committer) appendSegment(filename string, segment *segment) {
	c.segmentsLocker.Lock()
	defer c.segmentsLocker.Unlock()
	segment.refInc()
	c.segments[filepath.Base(filename)] = segment
	if err := c.indexTable.update1(segment); err != nil {
		log.Fatalf("%+v", err)
	}
}

func (c *committer) getSegmentByIndex(index int64, lockSync bool) *segment {
	c.segmentsLocker.Lock()
	defer c.segmentsLocker.Unlock()
	var segments []*segment
	for _, segment := range c.segments {
		segments = append(segments, segment)
	}
	if len(segments) == 0 {
		return nil
	}
	for _, segment := range c.segments {
		if segment.meta.From.Index <= index && index <= segment.meta.To.Index {
			segment.refInc()
			if lockSync {
				segment.GetSyncLocker().Lock()
			}
			return segment
		}
	}
	return nil
}

func (c *committer) getSegment(filename string) *segment {
	c.segmentsLocker.Lock()
	defer c.segmentsLocker.Unlock()
	segment, ok := c.segments[filepath.Base(filename)]
	if ok {
		segment.refInc()
	}
	return segment
}

func (c *committer) deleteSegment(filename string) error {
	filename = filepath.Base(filename)
	c.segmentsLocker.Lock()
	defer c.segmentsLocker.Unlock()
	segment, ok := c.segments[filename]
	if ok == false {
		return ErrNoFindSegment
	}
	delete(c.segments, filename)
	if err := c.indexTable.remove1(segment); err != nil {
		return err
	}
	segment.refDec()
	return nil
}

func (c *committer) flushCallback(filename string) error {
	segment, err := openSegment(filename)
	if err != nil {
		return errors.WithStack(err)
	}
	var remove *mStreamTable
	c.locker.Lock()
	if len(c.immutableMStreamMaps) > c.maxImmutableMStreamTableCount {
		remove = c.immutableMStreamMaps[0]
		copy(c.immutableMStreamMaps[0:], c.immutableMStreamMaps[1:])
		c.immutableMStreamMaps[len(c.immutableMStreamMaps)-1] = nil
		c.immutableMStreamMaps = c.immutableMStreamMaps[:len(c.immutableMStreamMaps)-1]
	}
	c.locker.Unlock()

	c.appendSegment(filename, segment)
	//remove from indexTable
	if remove != nil {
		for _, mStream := range remove.mStreams {
			c.indexTable.remove(mStream)
		}
	}
	if err := c.manifest.appendSegment(&pb.AppendSegment{Filename: filename}); err != nil {
		log.Fatalf(err.Error())
	}
	if err := c.sstore.clearJournal(); err != nil {
		log.Fatalf(err.Error())
	}
	return nil
}

//ReceiveSegmentFile receive segment file from master,and append segment to local sstore
func (c *committer) ReceiveSegmentFile(filename string) error {
	segment, err := openSegment(filename)
	if err != nil {
		return errors.WithStack(err)
	}

	//clear old segments
	var segmentFiles = c.manifest.getSegmentFiles()
	if len(segmentFiles) != 0 {
		sort.Strings(segmentFiles)
		lastSegmentIndex, err := parseFilenameIndex(segmentFiles[len(segmentFiles)-1])
		if err != nil {
			panic(err)
		}
		segmentIndex, err := parseFilenameIndex(filename)
		if lastSegmentIndex != segmentIndex-1 {
			for _, segmentFile := range segmentFiles {
				if segment := c.getSegment(segmentFile); segment != nil {
					_ = segment.deleteOnClose(true)
					segment.refDec()
					if err := c.deleteSegment(segmentFile); err != nil {
						log.Error(err)
					}
				}
			}
		}
	}

	//update end map
	for streamID, info := range segment.meta.OffSetInfos {
		c.enpMap.set(streamID, info.End, segment.meta.To)
	}

	//clear memory table
	c.mutableMStreamMap = newMStreamTable(c.enpMap, c.blockSize, len(segment.meta.OffSetInfos))

	//update version
	c.sstore.version = segment.meta.To

	if err := c.flushCallback(filename); err != nil {
		return err
	}

	for streamID, info := range segment.meta.OffSetInfos {
		item := notifyPool.Get().(*notify)
		item.streamID = streamID
		item.end = info.End
		c.endWatchers.notify(item)
	}
	return nil
}

func (c *committer) flush() {
	mStreamMap := c.mutableMStreamMap
	c.mutableMStreamMap = newMStreamTable(c.enpMap, c.blockSize,
		len(c.mutableMStreamMap.mStreams))
	c.locker.Lock()
	c.immutableMStreamMaps = append(c.immutableMStreamMaps, mStreamMap)
	c.locker.Unlock()
	c.flusher.append(mStreamMap, func(filename string, err error) {
		if err != nil {
			log.Fatal(err)
		}
		if err := c.flushCallback(filename); err != nil {
			log.Fatal(err)
		}
	})
}

func (c *committer) start() {
	c.cbWorker.start()
	c.flusher.start()
	go func() {
		for {
			items := c.queue.PopAll(nil)
			for i := range items {
				request := items[i]
				switch request := request.(type) {
				case *CloseRequest:
					c.flusher.close()
					c.callbackQueue.Push(request)
				case *WriteEntryRequest:
					mStream, end := c.mutableMStreamMap.appendEntry(request)
					if end == -1 {
						request.err = ErrOffset
						continue
					}
					request.end = end
					if mStream != nil {
						c.indexTable.update(mStream)
					}
					item := notifyPool.Get().(*notify)
					item.streamID = request.Entry.StreamID
					item.end = end
					c.endWatchers.notify(item)
					if c.mutableMStreamMap.mSize >= c.maxMStreamTableSize {
						c.flush()
					}
					c.callbackQueue.Push(request)
				}
			}
		}
	}()
}
