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
	"math"
	"path/filepath"
	"sync"
)

type journalWriter struct {
	journal        *journal
	queue          *block_queue.Queue
	commitQueue    *block_queue.Queue
	syncer         *Syncer
	manifest       *manifest
	maxJournalSize int64
}

func newJournalWriter(journal *journal,
	queue *block_queue.Queue,
	commitQueue *block_queue.Queue,
	syncer *Syncer,
	files *manifest, maxWalSize int64) *journalWriter {
	return &journalWriter{
		journal:        journal,
		queue:          queue,
		commitQueue:    commitQueue,
		syncer:         syncer,
		manifest:       files,
		maxJournalSize: maxWalSize,
	}
}

//append the writeRequest to the queue of writer
func (jWriter *journalWriter) append(e *writeRequest) {
	jWriter.queue.Push(e)
}

func (jWriter *journalWriter) JournalFilename() string {
	return filepath.Base(jWriter.journal.Filename())
}

func (jWriter *journalWriter) createNewJournal() error {
	index, err := jWriter.manifest.geNextJournal()
	if err != nil {
		return err
	}
	journal, err := openJournal(index)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := jWriter.manifest.AppendJournal(&pb.AppendJournal{Filename: index}); err != nil {
		return err
	}
	if err := jWriter.journal.Close(); err != nil {
		return err
	}
	header := jWriter.journal.GetMeta()
	header.Old = true
	if err := jWriter.manifest.setJournalHeader(header); err != nil {
		return err
	}
	jWriter.journal = journal
	jWriter.syncer.appendJournal(journal)
	return nil
}

const closeSignal = math.MinInt64

func (jWriter *journalWriter) start() {
	go func() {
		for {
			var writeRequests = objsPool.Get().([]interface{})[:0]
			entries := jWriter.queue.PopAll(nil)
			for i := range entries {
				e := entries[i]
				switch request := e.(type) {
				case *writeRequest:
					if jWriter.journal.Size() > jWriter.maxJournalSize {
						if err := jWriter.createNewJournal(); err != nil {
							request.cb(-1, err)
							continue
						}
					}
					if err := jWriter.journal.Write(request); err != nil {
						request.cb(-1, err)
					} else {
						writeRequests = append(writeRequests, request)
					}
				case *closeRequest:
					_ = jWriter.journal.Close()
					jWriter.commitQueue.Push(e)
					return
				}
			}
			if len(writeRequests) > 0 {
				if err := jWriter.journal.Flush(); err != nil {
					log.Fatal(err.Error())
				}
				jWriter.commitQueue.PushMany(writeRequests)
				jWriter.syncer.PushMany(writeRequests)
			}
		}
	}()
}

func (jWriter *journalWriter) close() {
	var wg sync.WaitGroup
	wg.Add(1)
	jWriter.queue.Push(&closeRequest{cb: func() {
		wg.Done()
	}})
	wg.Wait()
}