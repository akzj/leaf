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

type streamTable struct {
	locker    sync.Mutex
	size      int64
	from      *pb.Version // first version
	to        *pb.Version //last version, include
	endMap    *int64LockMap
	CreateTS  time.Time
	mStreams  map[int64]*stream
	blockSize int
}

func newStreamTable(sizeMap *int64LockMap,
	blockSize int, mStreamMapSize int) *streamTable {
	return &streamTable{
		locker:    sync.Mutex{},
		size:      0,
		from:      nil,
		to:        nil,
		endMap:    sizeMap,
		CreateTS:  time.Now(),
		mStreams:  make(map[int64]*stream, mStreamMapSize),
		blockSize: blockSize,
	}
}

func (m *streamTable) loadOrCreateStream(streamID int64) (*stream, bool) {
	m.locker.Lock()
	ms, ok := m.mStreams[streamID]
	if ok {
		m.locker.Unlock()
		return ms, true
	}
	size, _ := m.endMap.get(streamID)
	ms = newStream(size, m.blockSize, streamID)
	m.mStreams[streamID] = ms
	m.locker.Unlock()
	return ms, false
}

//appendEntry append *pb.Entry to stream,and return the stream if it create
func (m *streamTable) appendEntry(entry *pb.Entry, end *int64) (*stream, error) {
	ms, load := m.loadOrCreateStream(entry.StreamID)
	n, err := ms.WriteAt(entry.Data, entry.Offset)
	if err != nil {
		return nil, err
	}
	*end = int64(n) + ms.end
	m.endMap.set(entry.StreamID,*end, entry.Ver)
	m.size += int64(n)
	m.to = entry.Ver
	if m.from == nil {
		m.from = entry.Ver
	}
	if load {
		return nil, nil
	}
	return ms, nil
}
