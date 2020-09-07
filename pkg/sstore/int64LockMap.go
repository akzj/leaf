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
	"sync"
	"time"
)

type int64LockMap struct {
	version     Version
	locker      *sync.RWMutex
	cloneLocker *sync.Mutex
	level0      map[int64]int64
	level1      map[int64]int64
}

func newInt64LockMap() *int64LockMap {
	return &int64LockMap{
		locker:      new(sync.RWMutex),
		cloneLocker: new(sync.Mutex),
		level0:      nil,
		level1:      make(map[int64]int64, 1024),
	}
}

func (sizeMap *int64LockMap) set(streamID int64, pos int64, ver Version) {
	sizeMap.locker.Lock()
	sizeMap.version = ver
	if sizeMap.level0 != nil {
		sizeMap.level0[streamID] = pos
		sizeMap.locker.Unlock()
		return
	}
	sizeMap.level1[streamID] = pos
	sizeMap.locker.Unlock()
	return
}

func (sizeMap *int64LockMap) get(streamID int64) (int64, bool) {
	sizeMap.locker.RLock()
	if sizeMap.level0 != nil {
		if size, ok := sizeMap.level0[streamID]; ok {
			sizeMap.locker.RUnlock()
			return size, ok
		}
	}
	size, ok := sizeMap.level1[streamID]
	sizeMap.locker.RUnlock()
	return size, ok
}

func (sizeMap *int64LockMap) mergeMap(count int) bool {
	sizeMap.locker.Lock()
	for k, v := range sizeMap.level0 {
		if count--; count == 0 {
			sizeMap.locker.Unlock()
			return false
		}
		sizeMap.level1[k] = v
		delete(sizeMap.level0, k)
	}
	sizeMap.level0 = nil
	sizeMap.locker.Unlock()
	return true
}

func (sizeMap *int64LockMap) CloneMap() (map[int64]int64, Version) {
	sizeMap.cloneLocker.Lock()
	sizeMap.locker.Lock()
	sizeMap.level0 = make(map[int64]int64, 1024)
	cloneMap := make(map[int64]int64, len(sizeMap.level1))
	ver := sizeMap.version
	sizeMap.locker.Unlock()
	defer func() {
		go func() {
			defer sizeMap.cloneLocker.Unlock()
			for {
				if !sizeMap.mergeMap(20000) {
					time.Sleep(time.Millisecond * 10)
				}
			}
		}()
	}()
	for k, v := range sizeMap.level1 {
		cloneMap[k] = v
	}
	return cloneMap, ver
}
