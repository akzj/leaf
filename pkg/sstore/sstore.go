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
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type SStore struct {
	options    Options
	entryQueue *block_queue.Queue

	version     *pb.Version
	notifyPool  sync.Pool
	endMap      *int64LockMap
	committer   *committer
	indexTable  *indexTable
	endWatchers *endWatchers
	wWriter     *wWriter
	manifest    *manifest
	isClose     int32
}

type Snapshot struct {
	EndMap  map[int64]int64 `json:"end_map"`
	Version *pb.Version     `json:"version"`
}

func Open(options Options) (*SStore, error) {
	var sstore = &SStore{
		options:    options,
		entryQueue: block_queue.NewQueue(options.RequestQueueCap),
		notifyPool: sync.Pool{
			New: func() interface{} {
				return make(chan interface{}, 1)
			},
		},
		endMap:      newInt64LockMap(),
		indexTable:  newIndexTable(),
		endWatchers: newEndWatchers(),
		wWriter:     nil,
		manifest:    nil,
		isClose:     0,
	}

	if err := reload(sstore); err != nil {
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
	sstore.entryQueue.Push(&writeRequest{
		entry: &pb.Entry{
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
	if err := sstore.gcWal(); err != nil {
		return err
	}
	if err := sstore.gcSegment(); err != nil {
		return err
	}
	return nil
}

//Close sstore
func (sstore *SStore) Close() error {
	if atomic.CompareAndSwapInt32(&sstore.isClose, 0, 1) == false {
		return errors.New("repeated close")
	}
	sstore.wWriter.close()
	sstore.manifest.close()
	sstore.endWatchers.close()
	return nil
}

func (sstore *SStore) GetSnapshot() Snapshot {
	int64Map, version := sstore.endMap.CloneMap()
	return Snapshot{
		EndMap:  int64Map,
		Version: version,
	}
}

func (sstore *SStore) OpenSegment(name string) (*Segment, error) {
	segment := sstore.committer.getSegment(name)
	if segment == nil {
		return nil, ErrNoFindSegment
	}
	f, err := os.Open(segment.filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Segment{
		f: f,
		release: func() {
			segment.refDec()
		},
	}, nil
}

func (sstore *SStore) Sync(index int64) {
	segment := sstore.committer.getSegment2(index)
	if segment == nil {
		//todo sync from journal
	}
}

type Segment struct {
	f       *os.File
	release func()
}

func (s *Segment) Read(p []byte) (n int, err error) {
	return s.f.Read(p)
}

func (s *Segment) Seek(offset int64, whence int) (int64, error) {
	return s.f.Seek(offset, whence)
}

func (s *Segment) Close() error {
	err := s.Close()
	s.release()
	return err
}
