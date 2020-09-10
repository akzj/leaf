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
	wal        *journal
	queue      *block_queue.Queue
	commit     *block_queue.Queue
	files      *manifest
	maxWalSize int64
}

func newWWriter(w *journal, queue *block_queue.Queue, commitQueue *block_queue.Queue,
	files *manifest, maxWalSize int64) *wWriter {
	return &wWriter{
		wal:        w,
		queue:      queue,
		commit:     commitQueue,
		files:      files,
		maxWalSize: maxWalSize,
	}
}

//append the writeRequest to the queue of writer
func (worker *wWriter) append(e *writeRequest) {
	worker.queue.Push(e)
}

func (worker *wWriter) walFilename() string {
	return filepath.Base(worker.wal.Filename())
}

func (worker *wWriter) createNewWal() error {
	walFile, err := worker.files.getNextWal()
	if err != nil {
		return err
	}
	wal, err := openJournal(walFile)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := worker.files.appendWal(&pb.AppendWal{Filename: walFile}); err != nil {
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
			var writeRequests = objsPool.Get().([]interface{})[:0]
			entries := worker.queue.PopAll(nil)
			for i := range entries {
				e := entries[i]
				switch request := e.(type) {
				case *writeRequest:
					if worker.wal.Size() > worker.maxWalSize {
						if err := worker.createNewWal(); err != nil {
							request.cb(-1, err)
							continue
						}
					}
					if err := worker.wal.Write(request); err != nil {
						request.cb(-1, err)
					} else {
						writeRequests = append(writeRequests, request)
					}
				case *closeRequest:
					_ = worker.wal.Close()
					worker.commit.Push(e)
					return
				}
			}
			if len(writeRequests) > 0 {
				if err := worker.wal.Flush(); err != nil {
					log.Fatal(err.Error())
				}
				worker.commit.PushMany(writeRequests)
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
