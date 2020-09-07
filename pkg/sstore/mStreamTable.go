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

type mStreamTable struct {
	locker      sync.Mutex
	mSize       int64
	lastEntryID int64
	endMap      *int64LockMap
	GcTS        time.Time
	mStreams    map[int64]*mStream
	indexTable  *indexTable
	blockSize   int
}

func newMStreamTable(sizeMap *int64LockMap,
	blockSize int, mStreamMapSize int) *mStreamTable {
	return &mStreamTable{
		blockSize:   blockSize,
		mSize:       0,
		lastEntryID: 0,
		endMap:      sizeMap,
		locker:      sync.Mutex{},
		mStreams:    make(map[int64]*mStream, mStreamMapSize),
	}
}

func (m *mStreamTable) loadOrCreateMStream(streamID int64) (*mStream, bool) {
	m.locker.Lock()
	ms, ok := m.mStreams[streamID]
	if ok {
		m.locker.Unlock()
		return ms, true
	}
	size, _ := m.endMap.get(streamID)
	ms = newMStream(size, m.blockSize, streamID)
	m.mStreams[streamID] = ms
	m.locker.Unlock()
	return ms, false
}

//appendEntry append entry mStream,and return the mStream if it created
func (m *mStreamTable) appendEntry(e *entry) (*mStream, int64) {
	ms, load := m.loadOrCreateMStream(e.StreamID)
	end := ms.write(e.Offset, e.data)
	if end == -1 {
		return nil, -1
	}
	m.endMap.set(e.StreamID, end, e.ver)
	m.mSize += int64(len(e.data))
	m.lastEntryID = e.ID
	if load {
		return nil, end
	}
	return ms, end
}
