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
	log "github.com/sirupsen/logrus"
	"math"
	"path/filepath"
	"strconv"
)

type segmentFlusher struct {
	dir   string
	queue *block_queue.QueueWithContext
}

type flushSegment struct {
	rename      string
	streamTable *streamTable
	callback    func(filename string, err error)
}

func newSegmentFlusher(dir string, queue *block_queue.QueueWithContext) *segmentFlusher {
	return &segmentFlusher{
		dir:   dir,
		queue: queue,
	}
}

func (flusher *segmentFlusher) flush(table *streamTable) (string, error) {
	filename := filepath.Join(flusher.dir, strconv.FormatInt(math.MaxInt64, 10)+segmentExt)
	if err := flushStreamTable(filename, table); err != nil {
		return "", err
	}
	return filename, nil
}

func (flusher *segmentFlusher) flushLoop() {
	for {
		item, err := flusher.queue.Pop()
		if err != nil {
			log.Warn(err)
			return
		}
		flushSegment := item.(flushSegment)
		flushSegment.callback(flusher.flush(flushSegment.streamTable))
	}
}
