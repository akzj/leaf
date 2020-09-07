// Copyright 2020-2026 The sstore Authors
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
	"github.com/pkg/errors"
	"log"
	"math"
	"path/filepath"
	"sync"
)

type wWriter struct {
	wal        *journal
	queue      *entryQueue
	commit     *entryQueue
	files      *manifest
	maxWalSize int64
}

func newWWriter(w *journal, queue *entryQueue,
	commitQueue *entryQueue,
	files *manifest, maxWalSize int64) *wWriter {
	return &wWriter{
		wal:        w,
		queue:      queue,
		commit:     commitQueue,
		files:      files,
		maxWalSize: maxWalSize,
	}
}

//append the entry to the queue of writer
func (worker *wWriter) append(e *entry) {
	worker.queue.put(e)
}

func (worker *wWriter) walFilename() string {
	return filepath.Base(worker.wal.Filename())
}

func (worker *wWriter) createNewWal() error {
	walFile := worker.files.getNextWal()
	wal, err := openJournal(walFile)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := worker.files.appendWal(appendWal{Filename: walFile}); err != nil {
		return err
	}
	if err := worker.wal.Close(); err != nil {
		return err
	}
	header := worker.wal.GetMeta()
	header.Old = true
	if err := worker.files.setWalHeader(header); err != nil {
		return err
	}
	worker.wal = wal
	return nil
}

const closeSignal = math.MinInt64

func (worker *wWriter) start() {
	go func() {
		for {
			var commit = entriesPool.Get().([]*entry)[0:]
			entries := worker.queue.take()
			for i := range entries {
				e := entries[i]
				if e.ID == closeSignal {
					_ = worker.wal.Close()
					worker.commit.put(e)
					return
				}
				if worker.wal.Size() > worker.maxWalSize {
					if err := worker.createNewWal(); err != nil {
						e.cb(-1, err)
						continue
					}
				}
				if err := worker.wal.Write(e); err != nil {
					e.cb(-1, err)
				} else {
					commit = append(commit, e)
				}
			}
			if len(commit) > 0 {
				if err := worker.wal.Flush(); err != nil {
					log.Fatal(err.Error())
				}
				worker.commit.putEntries(commit)
			}
		}
	}()
}

func (worker *wWriter) close() {
	var wg sync.WaitGroup
	wg.Add(1)
	worker.queue.put(&entry{
		ID: closeSignal,
		cb: func(_ int64, err error) {
			wg.Done()
		},
	})
	wg.Wait()
}
