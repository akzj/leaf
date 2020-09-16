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
	"context"
	"github.com/akzj/streamIO/pkg/block-queue"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type SStore struct {
	options    Options
	entryQueue *block_queue.Queue

	version       *pb.Version
	notifyPool    sync.Pool
	endMap        *int64LockMap
	committer     *committer
	indexTable    *indexTable
	endWatchers   *endWatchers
	journalWriter *journalWriter
	manifest      *manifest
	isClose       int32
	syncer        *Syncer
}

type Snapshot struct {
	EndMap  map[int64]int64 `json:"end_map"`
	Version *pb.Version     `json:"version"`
}

func Open(options Options) (*SStore, error) {
	var sstore = &SStore{
		options:    options,
		entryQueue: block_queue.NewQueue(options.RequestQueueCap),
		version:    nil,
		notifyPool: sync.Pool{
			New: func() interface{} {
				return make(chan interface{}, 1)
			},
		},
		endMap:        newInt64LockMap(),
		committer:     nil,
		indexTable:    newIndexTable(),
		endWatchers:   newEndWatchers(),
		journalWriter: nil,
		manifest:      nil,
		isClose:       0,
		syncer:        nil,
	}
	if err := sstore.init(); err != nil {
		return nil, err
	}
	return sstore, nil
}

func (sstore *SStore) Options() Options {
	return sstore.options
}

func (sstore *SStore) nextEntryID() int64 {
	return atomic.AddInt64(&sstore.version.Index, 1)
}

//Append append the data to end of the stream
//return offset to the data
func (sstore *SStore) Append(streamID int64, data []byte, offset int64) (int64, error) {
	notify := sstore.notifyPool.Get().(chan interface{})
	var err error
	var newOffset int64
	sstore.AsyncAppend(streamID, data, offset, func(offset int64, e error) {
		err = e
		newOffset = offset
		notify <- struct{}{}
	})
	<-notify
	sstore.notifyPool.Put(notify)
	return newOffset, err
}

//AsyncAppend async append the data to end of the stream
func (sstore *SStore) AsyncAppend(streamID int64, data []byte, offset int64, cb func(offset int64, err error)) {
	sstore.entryQueue.Push(&WriteRequest{
		Entry: &pb.Entry{
			StreamID: streamID,
			Offset:   offset,
			Ver: &pb.Version{
				Term:  0,
				Index: sstore.nextEntryID(),
			},
			Data: data,
		},
		close: false,
		end:   0,
		err:   nil,
		cb:    cb,
	})
}

//AsyncAppend async append the data to end of the stream
func (sstore *SStore) AppendEntryWithCb(entry *pb.Entry, cb func(offset int64, err error)) {
	sstore.version = entry.Ver
	sstore.entryQueue.Push(&WriteRequest{
		Entry: entry,
		close: false,
		end:   0,
		err:   nil,
		cb:    cb,
	})
}

//Reader create Reader of the stream
func (sstore *SStore) Reader(streamID int64) (io.ReadSeeker, error) {
	return sstore.indexTable.reader(streamID)
}

//Watcher create watcher of the stream
func (sstore *SStore) Watcher(streamID int64) Watcher {
	return sstore.endWatchers.newEndWatcher(streamID)
}

//size return the end of stream.
//return _,false when the stream no exist
func (sstore *SStore) End(streamID int64) (int64, bool) {
	return sstore.endMap.get(streamID)
}

//base return the begin of stream.
//return 0,false when the stream no exist
func (sstore *SStore) Begin(streamID int64) (int64, bool) {
	offsetIndex := sstore.indexTable.get(streamID)
	if offsetIndex == nil {
		return 0, false
	}
	return offsetIndex.begin()
}

//Exist
//return true if the stream exist otherwise return false
func (sstore *SStore) Exist(streamID int64) bool {
	_, ok := sstore.Begin(streamID)
	return ok
}

//GC will delete useless journal manifest,segments
func (sstore *SStore) GC() error {
	if err := sstore.clearSegment(); err != nil {
		return err
	}
	return nil
}

//Close sstore
func (sstore *SStore) Close() error {
	if atomic.CompareAndSwapInt32(&sstore.isClose, 0, 1) == false {
		return errors.New("repeated close")
	}
	sstore.journalWriter.close()
	sstore.manifest.close()
	sstore.endWatchers.close()
	sstore.syncer.Close()
	return nil
}

func (sstore *SStore) GetSnapshot() Snapshot {
	int64Map, version := sstore.endMap.CloneMap()
	return Snapshot{
		EndMap:  int64Map,
		Version: version,
	}
}

func (sstore *SStore) Sync(ctx context.Context, ServerID int64, index int64, f func(SyncCallback) error) error {
	return sstore.syncer.SyncRequest(ctx, ServerID, index, f)
}

func (sstore *SStore) OpenSegmentReader(filename string) (*SegmentReader, error) {
	segment := sstore.committer.getSegment(filename)
	if segment == nil {
		return nil, ErrNoFindSegment
	}
	return sstore.syncer.OpenSegmentReader(segment)
}

func (sstore *SStore) CreateSegmentWriter(filename string) (*SegmentWriter, error) {
	segmentIndex, err := strconv.ParseInt(strings.Split(filename, ".")[0], 10, 64)
	if err != nil {
		return nil, err
	}
	if err := sstore.manifest.setSegmentIndex(segmentIndex); err != nil {
		return nil, err
	}
	filename = filepath.Join(sstore.options.JournalDir, filename)
	f, err := os.Create(filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	writer := &SegmentWriter{
		f: f,
		discard: func() error {
			_ = f.Close()
			return os.Remove(filename)
		},
		commit: func() error {
			if err := f.Close(); err != nil {
				return err
			}
			return sstore.committer.ReceiveSegmentFile(filename)
		},
	}
	return writer, nil
}

func (sstore *SStore) Version() *pb.Version {
	return sstore.version
}
