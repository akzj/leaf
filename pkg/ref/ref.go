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

package ref

import (
	"fmt"
	"math"
	"sync"
)

type RefCount struct {
	l sync.RWMutex
	c int32
	f func()
}

func NewRefCount(count int32, f func()) *RefCount {
	return &RefCount{
		c: count,
		f: f,
	}
}

func (ref *RefCount) SetReleaseFunc(f func()) {
	ref.l.Lock()
	ref.f = f
	ref.l.Unlock()
}

func (ref *RefCount) Count() int32 {
	ref.l.Lock()
	c := ref.c
	ref.l.Unlock()
	return c
}

func (ref *RefCount) DecRef() int32 {
	ref.l.Lock()
	if ref.c <= 0 {
		panic(fmt.Errorf("RefCount.c %d error", ref.c))
	}
	ref.c -= 1
	if ref.c == 0 {
		ref.c = math.MinInt32
		ref.f()
		ref.l.Unlock()
		return 0
	}
	ref.l.Unlock()
	return ref.c
}

func (ref *RefCount) IncRef() int32 {
	ref.l.Lock()
	ref.c += 1
	ref.l.Unlock()
	return ref.c
}
