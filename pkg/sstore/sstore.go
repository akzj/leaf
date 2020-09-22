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
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Store struct {
	options    Options
	entryQueue *block_queue.QueueWithContext
	isClose    int32

	version              *pb.Version
	endMap               *int64LockMap
	committer            *committer
	sectionsTable        *sectionsTable
	streamWatcher        *streamWatcher
	journalWriter        *journalWriter
	manifest             *Manifest
	syncer               *Syncer
	flusher              *segmentFlusher
	sectionsTableUpdater *sectionsTableUpdater
	callbackWorker       *callbackWorker

	immutableMStreamMapsLocker sync.Mutex
	immutableMStreamMaps       []*streamTable

	segmentsLocker sync.Mutex
	segments       map[string]*segment

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

type Snapshot struct {
	EndMap  map[int64]int64 `json:"end_map"`
	Version *pb.Version     `json:"version"`
}

type WriteEntry struct {
	Entry *pb.Entry
	end   int64
	err   error
	cb    func(end int64, err error)
}

func Open(options Options) (*Store, error) {
	ctx, cancel := context.WithCancel(options.Ctx)
	var sstore = &Store{
		options:                    options,
		entryQueue:                 nil,
		isClose:                    0,
		version:                    nil,
		endMap:                     newInt64LockMap(),
		committer:                  nil,
		sectionsTable:              newSectionsTable(),
		streamWatcher:              nil,
		journalWriter:              nil,
		manifest:                   nil,
		syncer:                     nil,
		flusher:                    nil,
		sectionsTableUpdater:       nil,
		callbackWorker:             nil,
		immutableMStreamMapsLocker: sync.Mutex{},
		immutableMStreamMaps:       nil,
		segmentsLocker:             sync.Mutex{},
		segments:                   map[string]*segment{},
		wg:                         sync.WaitGroup{},
		ctx:                        ctx,
		cancel:                     cancel,
	}
	if err := sstore.init(); err != nil {
		return nil, err
	}
	return sstore, nil
}

func (store *Store) Options() Options {
	return store.options
}

func (store *Store) nextEntryID() int64 {
	return atomic.AddInt64(&store.version.Index, 1)
}

//AsyncAppend async append the data to end of the stream
func (store *Store) AsyncAppend(streamID int64, data []byte, offset int64, cb func(offset int64, err error)) {
	if err := store.entryQueue.Push(&WriteEntry{
		Entry: &pb.Entry{
			StreamID: streamID,
			Offset:   offset,
			Ver: &pb.Version{
				Term:  0,
				Index: store.nextEntryID(),
			},
			Data: data,
		},
		cb: cb,
	}); err != nil {
		cb(-1, err)
	}
}

//AsyncAppend async append the data to end of the stream
func (store *Store) AppendEntryWithCb(entry *pb.Entry, cb func(offset int64, err error)) {
	store.version = entry.Ver
	store.entryQueue.Push(&WriteEntry{
		Entry: entry,
		end:   0,
		err:   nil,
		cb:    cb,
	})
}

//Reader create Reader of the stream
func (store *Store) Reader(streamID int64) (io.ReadSeeker, error) {
	return store.sectionsTable.reader(streamID)
}

//Watcher create Watcher of the stream
func (store *Store) Watcher(streamID int64) *Watcher {
	return store.streamWatcher.newEndWatcher(streamID)
}

//End return the end of stream.
//return _,false when the stream no exist
func (store *Store) End(streamID int64) (int64, bool) {
	return store.endMap.get(streamID)
}

//base return the begin of stream.
//return 0,false when the stream no exist
func (store *Store) Begin(streamID int64) (int64, bool) {
	sections := store.sectionsTable.get(streamID)
	if sections == nil {
		return 0, false
	}
	return sections.begin()
}

//Exist
//return true if the stream exist otherwise return false
func (store *Store) Exist(streamID int64) bool {
	_, ok := store.Begin(streamID)
	return ok
}

//GC will delete useless journal manifest,segments
func (store *Store) GC() error {
	if err := store.clearSegment(); err != nil {
		return err
	}
	return nil
}

//Close store
func (store *Store) Close() error {
	if atomic.CompareAndSwapInt32(&store.isClose, 0, 1) == false {
		return errors.New("repeated close")
	}
	store.cancel()
	store.wg.Wait()
	return nil
}

func (store *Store) GetSnapshot() Snapshot {
	int64Map, version := store.endMap.CloneMap()
	return Snapshot{
		EndMap:  int64Map,
		Version: version,
	}
}

func (store *Store) getSegmentByIndex(index int64) *segment {
	store.segmentsLocker.Lock()
	defer store.segmentsLocker.Unlock()
	var segments []*segment
	for _, segment := range store.segments {
		segments = append(segments, segment)
	}
	if len(segments) == 0 {
		return nil
	}
	for _, segment := range store.segments {
		if segment.meta.From.Index <= index && index <= segment.meta.To.Index {
			segment.IncRef()
			return segment
		}
	}
	return nil
}

func (store *Store) getSegment(filename string) *segment {
	store.segmentsLocker.Lock()
	defer store.segmentsLocker.Unlock()
	segment, ok := store.segments[filepath.Base(filename)]
	if ok {
		segment.IncRef()
	}
	return segment
}

func (store *Store) appendSegment(filename string, segment *segment) {
	store.segmentsLocker.Lock()
	defer store.segmentsLocker.Unlock()
	segment.IncRef()
	store.segments[filepath.Base(filename)] = segment
	if err := store.sectionsTable.update1(segment); err != nil {
		log.Fatalf("%+v", err)
	}
}

func (store *Store) appendStreamTable(streamMap *streamTable) {
	store.immutableMStreamMapsLocker.Lock()
	defer store.immutableMStreamMapsLocker.Unlock()
	store.immutableMStreamMaps = append(store.immutableMStreamMaps, streamMap)
}

func (store *Store) flushCallback(filename string) error {
	nextSegment, err := store.manifest.GetNextSegment()
	if err != nil {
		return err
	}
	if err := os.Rename(filename, nextSegment); err != nil {
		return errors.WithStack(err)
	}
	if err := store.manifest.AppendSegment(&pb.AppendSegment{Filename: nextSegment}); err != nil {
		return err
	}
	segment, err := openSegment(nextSegment)
	if err != nil {
		return errors.WithStack(err)
	}
	var remove *streamTable
	store.immutableMStreamMapsLocker.Lock()
	if len(store.immutableMStreamMaps) > store.options.MaxImmutableMStreamTableCount {
		remove = store.immutableMStreamMaps[0]
		copy(store.immutableMStreamMaps[0:], store.immutableMStreamMaps[1:])
		store.immutableMStreamMaps[len(store.immutableMStreamMaps)-1] = nil
		store.immutableMStreamMaps = store.immutableMStreamMaps[:len(store.immutableMStreamMaps)-1]
	}
	store.immutableMStreamMapsLocker.Unlock()

	store.appendSegment(nextSegment, segment)
	//remove from sectionsTable
	if remove != nil {
		for _, mStream := range remove.streams {
			store.sectionsTable.remove(mStream)
		}
	}
	if err := store.clearJournal(); err != nil {
		log.Fatalf(err.Error())
	}
	return nil
}

func (store *Store) Sync(ctx context.Context, ServerID int64, index int64, f func(SyncCallback) error) error {
	return store.syncer.SyncRequest(ctx, ServerID, index, f)
}

func (store *Store) deleteSegment(filename string) error {
	filename = filepath.Base(filename)
	store.segmentsLocker.Lock()
	defer store.segmentsLocker.Unlock()
	segment, ok := store.segments[filename]
	if ok == false {
		return ErrNoFindSegment
	}
	delete(store.segments, filename)
	if err := store.sectionsTable.remove1(segment); err != nil {
		return err
	}
	segment.DecRef()
	return nil
}

func (store *Store) OpenSegmentReader(filename string) (*SegmentReader, error) {
	segment := store.getSegment(filename)
	if segment == nil {
		return nil, ErrNoFindSegment
	}
	return store.syncer.OpenSegmentReader(segment)
}

func (store *Store) commitSegmentFile(filename string) error {
	segment, err := openSegment(filename)
	if err != nil {
		return errors.WithStack(err)
	}

	//clear old segments
	var segmentFiles = store.manifest.GetSegmentFiles()
	if len(segmentFiles) != 0 {
		sort.Strings(segmentFiles)
		lastSegmentIndex, err := parseFileID(segmentFiles[len(segmentFiles)-1])
		if err != nil {
			panic(err)
		}
		segmentIndex, err := parseFileID(filename)
		if lastSegmentIndex != segmentIndex-1 {
			for _, segmentFile := range segmentFiles {
				if segment := store.getSegment(segmentFile); segment != nil {
					_ = segment.deleteOnClose(true)
					segment.DecRef()
					if err := store.deleteSegment(segmentFile); err != nil {
						log.Error(err)
					}
				}
			}
		}
	}

	//update end map
	for streamID, info := range segment.meta.OffSetInfos {
		store.endMap.set(streamID, info.End, segment.meta.To)
	}

	//clear memory sectionsMap
	store.committer.streamTable = newStreamTable(store.endMap,
		store.options.BlockSize, len(segment.meta.OffSetInfos))

	//update version
	store.version = segment.meta.To

	if err := store.flushCallback(filename); err != nil {
		return err
	}
	var notifies = make([]interface{}, 0, len(segment.meta.OffSetInfos))
	for streamID, info := range segment.meta.OffSetInfos {
		var item notify
		item.streamID = streamID
		item.end = info.End
		notifies = append(notifies, item)
	}
	_ = store.streamWatcher.queue.PushMany(notifies)
	return nil
}

func (store *Store) CreateSegmentWriter(filename string) (*SegmentWriter, error) {
	segmentID, err := strconv.ParseInt(strings.Split(filename, ".")[0], 10, 64)
	if err != nil {
		return nil, err
	}
	if err := store.manifest.SetSegmentID(segmentID); err != nil {
		return nil, err
	}
	filename = filepath.Join(store.options.SegmentDir, filename)
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
			return store.commitSegmentFile(filename)
		},
	}
	return writer, nil
}

func (store *Store) Version() *pb.Version {
	return store.version
}
