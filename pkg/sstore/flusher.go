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
	log "github.com/sirupsen/logrus"
)

type flusher struct {
	manifest *Manifest
	queue    *block_queue.QueueWithContext
}

type flushSegment struct {
	mStreamTable *streamTable
	callback     func(filename string, err error)
}

func newFlusher(manifest *Manifest, queue *block_queue.QueueWithContext) *flusher {
	return &flusher{
		manifest: manifest,
		queue:    queue,
	}
}

func (flusher *flusher) flushMStreamTable(table *streamTable) (string, error) {
	var filename, _ = flusher.manifest.GetNextSegment()
	segment, err := createSegment(filename)
	if err != nil {
		return "", err
	}
	if err := segment.flushMStreamTable(table); err != nil {
		return "", err
	}
	if err := segment.close(); err != nil {
		return "", err
	}
	return filename, nil
}

func (flusher *flusher) flushLoop() {
	for {
		item, err := flusher.queue.Pop()
		if err != nil {
			log.Warn(err)
			return
		}
		flushSegment := item.(flushSegment)
		flushSegment.callback(flusher.flushMStreamTable(flushSegment.mStreamTable))
	}
}
