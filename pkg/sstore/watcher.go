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
	"sync"
)

type Watcher struct {
	index int64
	c     func()
	posCh chan int64
}

type notify struct {
	streamID int64
	end      int64
}

func (watcher *Watcher) notify(pos int64) {
	select {
	case watcher.posCh <- pos:
	default:
	}
}

func (watcher *Watcher) Watch() chan int64 {
	return watcher.posCh
}

func (watcher *Watcher) Close() {
	if watcher.c != nil {
		watcher.c()
		watcher.c = nil
	}
}

type streamWatcher struct {
	watchIndex     int64
	watcherMap     map[int64][]Watcher
	endWatcherLock *sync.RWMutex
	queue          *block_queue.QueueWithContext
}

func newStreamWatcher(queue *block_queue.QueueWithContext) *streamWatcher {
	return &streamWatcher{
		watchIndex:     0,
		watcherMap:     make(map[int64][]Watcher),
		endWatcherLock: new(sync.RWMutex),
		queue:          queue,
	}
}

func (streamWatcher *streamWatcher) removeEndWatcher(index int64, streamID int64) {
	streamWatcher.endWatcherLock.Lock()
	defer streamWatcher.endWatcherLock.Unlock()
	endWatcherS, ok := streamWatcher.watcherMap[streamID]
	if ok == false {
		return
	}
	for i, w := range endWatcherS {
		if w.index == index {
			copy(endWatcherS[i:], endWatcherS[i+1:])
			endWatcherS[len(endWatcherS)-1] = Watcher{}
			endWatcherS = endWatcherS[:len(endWatcherS)-1]
			streamWatcher.watcherMap[streamID] = endWatcherS
			break
		}
	}
}

func (streamWatcher *streamWatcher) newEndWatcher(streamID int64) *Watcher {
	streamWatcher.endWatcherLock.Lock()
	defer streamWatcher.endWatcherLock.Unlock()
	streamWatcher.watchIndex++
	index := streamWatcher.watchIndex
	watcher := Watcher{
		index: index,
		posCh: make(chan int64, 1),
		c: func() {
			streamWatcher.removeEndWatcher(index, streamID)
		},
	}
	streamWatcher.watcherMap[streamID] = append(streamWatcher.watcherMap[streamID], watcher)
	return &watcher
}

func (streamWatcher *streamWatcher) getWatcher(streamID int64) []Watcher {
	streamWatcher.endWatcherLock.RLock()
	watcher, _ := streamWatcher.watcherMap[streamID]
	streamWatcher.endWatcherLock.RUnlock()
	return watcher
}

func (streamWatcher *streamWatcher) notifyLoop() {
	for {
		items, err := streamWatcher.queue.PopAll(nil)
		if err != nil {
			log.Warn(err)
			return
		}
		for _, item := range items {
			item := item.(notify)
			for _, watcher := range streamWatcher.getWatcher(item.streamID) {
				watcher.notify(item.end)
			}
		}
	}
}
