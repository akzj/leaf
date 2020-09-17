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
)

type cbWorker struct {
	queue *block_queue.Queue
}

func newCbWorker(queue *block_queue.Queue) *cbWorker {
	return &cbWorker{queue: queue}
}

func (worker *cbWorker) start() {
	go func() {
		for {
			items := worker.queue.PopAll(nil)
			for index := range items {
				item := items[index]
				items[index] = nil
				switch request := item.(type) {
				case *WriteRequest:
					request.cb(request.end, request.err)
				case *closeRequest:
					request.cb()
					return
				}
			}
		}
	}()
}
