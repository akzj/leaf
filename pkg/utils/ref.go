package utils

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
