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

type mStreamTable struct {
	locker    sync.Mutex
	mSize     int64
	from      *pb.Version // first version
	to        *pb.Version //last version, include
	endMap    *int64LockMap
	CreateTS  time.Time
	mStreams  map[int64]*mStream
	blockSize int
}

func newMStreamTable(sizeMap *int64LockMap,
	blockSize int, mStreamMapSize int) *mStreamTable {
	return &mStreamTable{
		locker:    sync.Mutex{},
		mSize:     0,
		from:      nil,
		to:        nil,
		endMap:    sizeMap,
		CreateTS:  time.Now(),
		mStreams:  make(map[int64]*mStream, mStreamMapSize),
		blockSize: blockSize,
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

//appendEntry append writeRequest mStream,and return the mStream if it created
func (m *mStreamTable) appendEntry(e *writeRequest) (*mStream, int64) {
	ms, load := m.loadOrCreateMStream(e.entry.StreamID)
	end := ms.write(e.entry.Offset, e.entry.Data)
	if end == -1 {
		return nil, -1
	}
	m.endMap.set(e.entry.StreamID, end, e.entry.Ver)
	m.mSize += int64(len(e.entry.Data))
	m.to = e.entry.Ver
	if m.from == nil {
		m.from = e.entry.Ver
	}
	if load {
		return nil, end
	}
	return ms, end
}
