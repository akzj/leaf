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
	"bufio"
	"bytes"
	"context"
	"github.com/akzj/streamIO/pkg/block-queue"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type Syncer struct {
	sstore          *Store
	journalAppendCh chan interface{}

	journalLocker sync.Mutex
	journals      []*journal

	subscribersLocker sync.Mutex
	subscribers       map[int64]*subscriber
}

type subscriber struct {
	nextIndex *int64
	bytes     int64
	notifyCh  chan interface{}
}

type SyncCallback struct {
	Index      *int64
	Segment    *SegmentReader
	EntryQueue *block_queue.QueueWithContext
}

func newSubscriber(ctx context.Context, index *int64) *subscriber {
	return &subscriber{
		nextIndex: index,
		bytes:     1024,
		notifyCh:  make(chan interface{}, 1),
	}
}

func newSyncer(sstore *Store) *Syncer {
	return &Syncer{
		sstore:            sstore,
		journalAppendCh:   make(chan interface{}, 1),
		journalLocker:     sync.Mutex{},
		journals:          nil,
		subscribersLocker: sync.Mutex{},
		subscribers:       map[int64]*subscriber{},
	}
}

func (syncer *Syncer) appendJournal(journal *journal) {
	syncer.journalLocker.Lock()
	defer syncer.journalLocker.Unlock()
	syncer.journals = append(syncer.journals, journal)
}

func (syncer *Syncer) DeleteJournal(filename string) {
	syncer.journalLocker.Lock()
	defer syncer.journalLocker.Unlock()
	for index, journal := range syncer.journals {
		if journal.filename == filename {
			journal.RefCount.SetReleaseFunc(func() {
				_ = journal.Close()
				if err := os.Remove(journal.filename); err != nil {
					log.Fatal(err)
				}
			})
			journal.DecRef()
			syncer.journals = append(syncer.journals[:index], syncer.journals[index+1:]...)
			break
		}
	}
}
func (syncer *Syncer) deleteSubscriber(serviceID int64) {
	syncer.subscribersLocker.Lock()
	defer syncer.subscribersLocker.Unlock()
	delete(syncer.subscribers, serviceID)
}

func (syncer *Syncer) syncJournal(ctx context.Context, index *int64,
	journal *journal, f func(callback SyncCallback) error) error {

	defer journal.DecRef()
	begin, err := journal.index.find(atomic.LoadInt64(index))
	if err != nil {
		panic(err)
	}

	var reader io.Reader
	journalMMap := journal.GetJournalMMap()
	if journalMMap != nil {
		defer journalMMap.DecRef()
		bufReader := bytes.NewReader(journalMMap.data[:mmapSize])
		if _, err := bufReader.Seek(begin.Offset, io.SeekStart); err != nil {
			return err
		}
		if begin.Offset >= mmapSize {
			log.Panic(begin.Offset)
		}
		reader = bufReader
	} else {
		file, err := os.Open(journal.filename)
		if err != nil {
			return err
		}
		defer func() {
			_ = file.Close()
		}()
		if _, err := file.Seek(begin.Offset, io.SeekStart); err != nil {
			return err
		}
		reader = bufio.NewReaderSize(file, 1024*1024)
	}

	var queue = block_queue.NewQueueWithContext(ctx, 1024)
	var errs = make(chan error)
	go func() {
		errs <- f(SyncCallback{EntryQueue: queue})
		log.Debug("syncJournal done ", journal.filename)
	}()
	var count int64
	defer func() {
		log.Debug("index", atomic.LoadInt64(index))
	}()
	for count = journal.GetFlushIndex() - atomic.LoadInt64(index); count >= 0; count-- {
		batchEntry, err := DecodeBatchEntry(reader)
		if err != nil {
			log.Error(err)
			if err == io.EOF {
				queue.Close(nil)
				break
			}
			if err == io.ErrUnexpectedEOF {
				queue.Close(nil)
				break
			}
			return err
		}
		if batchEntry.Ver.Index != atomic.LoadInt64(index) {
			log.Panic(*index, batchEntry.Ver)
		}
		atomic.AddInt64(index, 1)
		if err := queue.Push(batchEntry); err != nil {
			return err
		}
	}
	queue.Close(nil)
	select {
	case err := <-errs:
		return err
	}
}

func (syncer *Syncer) SyncRequest(ctx context.Context, serverID, index int64, f func(SyncCallback) error) error {
	log.Debug("SyncRequest", serverID, index)
	//sync from segment
	segment := syncer.sstore.getSegmentByIndex(index)
	if segment != nil {
		reader, err := syncer.OpenSegmentReader(segment)
		if err != nil {
			return err
		}
		return f(SyncCallback{
			Segment: reader,
		})
	}

	syncer.subscribersLocker.Lock()
	sub, ok := syncer.subscribers[serverID]
	if ok == false {
		sub = newSubscriber(ctx, &index)
		syncer.subscribers[serverID] = sub
	}
	syncer.subscribersLocker.Unlock()

	defer func() {
		syncer.deleteSubscriber(serverID)
	}()

	//sync from journal
	for {
		var journal *journal
		syncer.journalLocker.Lock()
		for _, journalIter := range syncer.journals {
			if index >= journalIter.meta.From.Index && index <= journalIter.GetFlushIndex() {
				log.Debug("Find", journalIter.meta, "flushIndex", journalIter.GetFlushIndex(), "index", index)
				journal = journalIter
				journal.IncRef()
				break
			}
		}
		syncer.journalLocker.Unlock()
		if journal != nil {
			if err := syncer.syncJournal(ctx, &index, journal, f); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				log.Warn(ctx.Err())
				return nil
			default:
			}
		} else {
			select {
			case <-sub.notifyCh:
			case <-ctx.Done():
				log.Warn(ctx.Err())
				return nil
			}
		}
	}
}

func (syncer *Syncer) notifySubscriberLoop() {
	for {
		select {
		case <-syncer.journalAppendCh:
		case <-syncer.sstore.ctx.Done():
			log.Warning(syncer.sstore.ctx.Err())
			return
		}
		syncer.subscribersLocker.Lock()
		if len(syncer.subscribers) == 0 {
			syncer.subscribersLocker.Unlock()
			continue
		}
		for _, subscriber := range syncer.subscribers {
			select {
			case <-subscriber.notifyCh:
			default:
			}
		}
		syncer.subscribersLocker.Unlock()
	}
}

func (syncer *Syncer) OpenSegmentReader(segment *segment) (*SegmentReader, error) {
	f, err := os.Open(segment.filename)
	if err != nil {
		segment.DecRef()
		return nil, errors.WithStack(err)
	}
	return &SegmentReader{
		f:        f,
		filename: segment.filename,
		isClose:  0,
		release: func() {
			segment.DecRef()
		},
	}, nil
}
