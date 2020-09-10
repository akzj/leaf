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
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"sync"
	"time"
)

type int64LockMap struct {
	version     *pb.Version
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

func (int64Map *int64LockMap) set(streamID int64, pos int64, ver *pb.Version) {
	int64Map.locker.Lock()
	int64Map.version = ver
	if int64Map.level0 != nil {
		int64Map.level0[streamID] = pos
		int64Map.locker.Unlock()
		return
	}
	int64Map.level1[streamID] = pos
	int64Map.locker.Unlock()
	return
}

func (int64Map *int64LockMap) get(streamID int64) (int64, bool) {
	int64Map.locker.RLock()
	if int64Map.level0 != nil {
		if size, ok := int64Map.level0[streamID]; ok {
			int64Map.locker.RUnlock()
			return size, ok
		}
	}
	size, ok := int64Map.level1[streamID]
	int64Map.locker.RUnlock()
	return size, ok
}

func (int64Map *int64LockMap) mergeMap(count int) bool {
	int64Map.locker.Lock()
	for k, v := range int64Map.level0 {
		if count--; count == 0 {
			int64Map.locker.Unlock()
			return false
		}
		int64Map.level1[k] = v
		delete(int64Map.level0, k)
	}
	int64Map.level0 = nil
	int64Map.locker.Unlock()
	return true
}

func (int64Map *int64LockMap) CloneMap() (map[int64]int64, *pb.Version) {
	int64Map.cloneLocker.Lock()
	int64Map.locker.Lock()
	int64Map.level0 = make(map[int64]int64, 1024)
	cloneMap := make(map[int64]int64, len(int64Map.level1))
	ver := int64Map.version
	int64Map.locker.Unlock()
	defer func() {
		go func() {
			defer int64Map.cloneLocker.Unlock()
			for {
				if !int64Map.mergeMap(20000) {
					time.Sleep(time.Millisecond * 10)
				}
			}
		}()
	}()
	for k, v := range int64Map.level1 {
		cloneMap[k] = v
	}
	return cloneMap, ver
}
