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

type wWriter struct {
	journal        *journal
	queue          *block_queue.Queue
	commitQueue    *block_queue.Queue
	syncer         *Syncer
	manifest       *manifest
	maxJournalSize int64
}

func newWWriter(journal *journal,
	queue *block_queue.Queue,
	commitQueue *block_queue.Queue,
	syncer *Syncer,
	files *manifest, maxWalSize int64) *wWriter {
	return &wWriter{
		journal:        journal,
		queue:          queue,
		commitQueue:    commitQueue,
		syncer:         syncer,
		manifest:       files,
		maxJournalSize: maxWalSize,
	}
}

//append the writeRequest to the queue of writer
func (worker *wWriter) append(e *writeRequest) {
	worker.queue.Push(e)
}

func (worker *wWriter) JournalFilename() string {
	return filepath.Base(worker.journal.Filename())
}

func (worker *wWriter) createNewJournal() error {
	index, err := worker.manifest.geNextJournal()
	if err != nil {
		return err
	}
	journal, err := openJournal(index)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := worker.manifest.AppendJournal(&pb.AppendJournal{Filename: index}); err != nil {
		return err
	}
	if err := worker.journal.Close(); err != nil {
		return err
	}
	header := worker.journal.GetMeta()
	header.Old = true
	if err := worker.manifest.setJournalHeader(header); err != nil {
		return err
	}
	worker.journal = journal
	worker.syncer.appendJournal(journal)
	return nil
}

const closeSignal = math.MinInt64

func (worker *wWriter) start() {
	go func() {
		for {
			var writeRequests = objsPool.Get().([]interface{})[:0]
			entries := worker.queue.PopAll(nil)
			for i := range entries {
				e := entries[i]
				switch request := e.(type) {
				case *writeRequest:
					if worker.journal.Size() > worker.maxJournalSize {
						if err := worker.createNewJournal(); err != nil {
							request.cb(-1, err)
							continue
						}
					}
					if err := worker.journal.Write(request); err != nil {
						request.cb(-1, err)
					} else {
						writeRequests = append(writeRequests, request)
					}
				case *closeRequest:
					_ = worker.journal.Close()
					worker.commitQueue.Push(e)
					return
				}
			}
			if len(writeRequests) > 0 {
				if err := worker.journal.Flush(); err != nil {
					log.Fatal(err.Error())
				}
				worker.commitQueue.PushMany(writeRequests)
				worker.syncer.PushMany(writeRequests)
			}
		}
	}()
}

func (worker *wWriter) close() {
	var wg sync.WaitGroup
	wg.Add(1)
	worker.queue.Push(&closeRequest{cb: func() {
		wg.Done()
	}})
	wg.Wait()
}
