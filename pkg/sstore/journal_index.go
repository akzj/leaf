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
	"fmt"
	"github.com/pkg/errors"
	"sync"
)

type journalOffset struct {
	Offset int64
	Index  int64
}

func (JI *journalOffset) String() string {
	return fmt.Sprintf("index:%d,offset:%d", JI.Index, JI.Offset)
}

func (indexes *journalIndex) append(offset journalOffset) {
	indexes.locker.Lock()
	indexes.jIndexes = append(indexes.jIndexes, offset)
	indexes.locker.Unlock()
}

type journalIndex struct {
	locker   sync.RWMutex
	jIndexes []journalOffset
}

func (indexes *journalIndex) find(index int64) (*journalOffset, error) {
	indexes.locker.RLock()
	if len(indexes.jIndexes) != 0 {
		from := indexes.jIndexes[0].Index
		to := indexes.jIndexes[len(indexes.jIndexes)-1].Index
		if index < indexes.jIndexes[0].Index || index > to {
			indexes.locker.RUnlock()
			return nil, errors.Errorf("index %d Out of range[%d,%d]", index, from, to)
		}
		offset := index - indexes.jIndexes[0].Index
		jIndex := indexes.jIndexes[offset]
		indexes.locker.RUnlock()
		return &jIndex, nil
	}
	indexes.locker.RUnlock()
	return nil, errors.Errorf("journal index empty")
}
