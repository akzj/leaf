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
	block_queue "github.com/akzj/streamIO/pkg/block-queue"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	"log"
	"path/filepath"
	"sort"
	"sync"
)

type committer struct {
	queue *block_queue.Queue

	maxMStreamTableSize int64
	mutableMStreamMap   *mStreamTable
	sizeMap             *int64LockMap

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

	cbWorker             *cbWorker
	callbackQueue        *block_queue.Queue
	flushSegmentCallback func(filename string)
}

func newCommitter(options Options,
	endWatchers *endWatchers,
	indexTable *indexTable,
	sizeMap *int64LockMap,
	mutableMStreamMap *mStreamTable,
	queue *block_queue.Queue,
	manifest *manifest,
	blockSize int,
	flushSegmentCallback func(filename string)) *committer {

	cbQueue := block_queue.NewQueue(128)

	return &committer{
		queue:                         queue,
		maxMStreamTableSize:           options.MaxMStreamTableSize,
		mutableMStreamMap:             mutableMStreamMap,
		sizeMap:                       sizeMap,
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
		flushSegmentCallback:          flushSegmentCallback,
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

func (c *committer) getSegment2(index int64) *segment {
	c.segmentsLocker.Lock()
	defer c.segmentsLocker.Unlock()
	var segments []*segment
	for _, segment := range c.segments {
		segments = append(segments, segment)
	}
	if len(segments) == 0 {
		return nil
	}
	sort.Slice(segments, func(i, j int) bool {
		if len(segments[i].filename) != len(segments[i].filename) {
			return len(segments[i].filename) < len(segments[i].filename)
		}
		return segments[i].filename < segments[i].filename
	})
	i := sort.Search(len(segments), func(i int) bool {
		return index <= segments[i].meta.From.Index
	})
	if i < len(segments) && segments[i].meta.From.Index == index {
		segment := segments[i]
		segment.refInc()
		return segment
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

func (c *committer) AppendSegmentFile(filename string) error {
	if err := c.flushCallback(filename); err != nil {
		return err
	}
	segment := c.getSegment(filename)
	if segment == nil {
		return errors.Errorf("no find segment %s", filename)
	}
	for streamID, info := range segment.meta.OffSetInfos {
		item := notifyPool.Get().(*notify)
		item.streamID = streamID
		item.end = info.End
		c.endWatchers.notify(item)
	}
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
		return err
	}
	c.flushSegmentCallback(filename)
	return err
}

func (c *committer) flush() {
	mStreamMap := c.mutableMStreamMap
	c.mutableMStreamMap = newMStreamTable(c.sizeMap, c.blockSize,
		len(c.mutableMStreamMap.mStreams))
	c.locker.Lock()
	c.immutableMStreamMaps = append(c.immutableMStreamMaps, mStreamMap)
	c.locker.Unlock()
	c.flusher.append(mStreamMap, func(filename string, err error) {
		if err != nil {
			log.Fatal(err.Error())
		}
		c.flushCallback(filename)
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
				case *closeRequest:
					c.flusher.close()
					c.callbackQueue.Push(request)
				case *writeRequest:
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
					item.streamID = request.entry.StreamID
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
