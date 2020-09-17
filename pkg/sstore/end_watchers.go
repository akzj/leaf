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
	"sync"
)

type endWatcher struct {
	index  int64
	c      func()
	int64s chan int64
}

type notify struct {
	streamID int64
	end      int64
}

type endWatchers struct {
	watchIndex     int64
	endWatcherMap  map[int64][]endWatcher
	endWatcherLock *sync.RWMutex
	queue          *block_queue.Queue
}

var notifyPool = sync.Pool{New: func() interface{} {
	return new(notify)
}}

func newEndWatchers() *endWatchers {
	return &endWatchers{
		watchIndex:     0,
		endWatcherMap:  make(map[int64][]endWatcher),
		endWatcherLock: new(sync.RWMutex),
		queue:          block_queue.NewQueue(1024),
	}
}
func (endWatchers *endWatchers) removeEndWatcher(index int64, streamID int64) {
	endWatchers.endWatcherLock.Lock()
	defer endWatchers.endWatcherLock.Unlock()
	endWatcherS, ok := endWatchers.endWatcherMap[streamID]
	if ok == false {
		return
	}
	for i, watcher := range endWatcherS {
		if watcher.index == index {
			copy(endWatcherS[i:], endWatcherS[i+1:])
			endWatcherS[len(endWatcherS)-1] = endWatcher{}
			endWatcherS = endWatcherS[:len(endWatcherS)-1]
			endWatchers.endWatcherMap[streamID] = endWatcherS
			break
		}
	}
}

func (endWatchers *endWatchers) newEndWatcher(streamID int64) *endWatcher {
	endWatchers.endWatcherLock.Lock()
	defer endWatchers.endWatcherLock.Unlock()
	endWatchers.watchIndex++
	index := endWatchers.watchIndex
	watcher := endWatcher{
		index:  index,
		int64s: make(chan int64, 1),
		c: func() {
			endWatchers.removeEndWatcher(index, streamID)
		},
	}
	endWatchers.endWatcherMap[streamID] = append(endWatchers.endWatcherMap[streamID], watcher)
	return &watcher
}

func (endWatchers *endWatchers) getEndWatcher(streamID int64) []endWatcher {
	endWatchers.endWatcherLock.RLock()
	watcher, _ := endWatchers.endWatcherMap[streamID]
	endWatchers.endWatcherLock.RUnlock()
	return watcher
}

func (endWatchers *endWatchers) start() {
	go func() {
		var buf = make([]interface{}, 0, 1024)
		for {
			items := endWatchers.queue.PopAll(buf)
			for _, item := range items {
				switch item := item.(type) {
				case *notify:
					for _, watcher := range endWatchers.getEndWatcher(item.streamID) {
						watcher.notify(item.end)
					}
					notifyPool.Put(item)
				case *closeRequest:
					item.cb()
					return
				}
			}
		}
	}()
}

func (endWatchers *endWatchers) close() {
	var wg sync.WaitGroup
	wg.Add(1)
	endWatchers.queue.Push(&closeRequest{cb: func() {
		wg.Done()
	}})
	wg.Wait()
}

func (endWatchers *endWatchers) notify(item *notify) {
	endWatchers.queue.Push(item)
}

func (watcher *endWatcher) notify(pos int64) {
	select {
	case watcher.int64s <- pos:
	default:
	}
}

func (watcher *endWatcher) Watch() chan int64 {
	return watcher.int64s
}

func (watcher *endWatcher) Close() {
	if watcher.c != nil {
		watcher.c()
		watcher.c = nil
	}
}
