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

package block_queue

import (
	"context"
	"github.com/pkg/errors"
	"sync"
	"sync/atomic"
)

type Queue struct {
	max    int
	empty  *sync.Cond
	full   *sync.Cond
	locker *sync.Mutex
	pos    int
	items  []interface{}
}

func NewQueue(maxSize int) *Queue {
	locker := new(sync.Mutex)
	return &Queue{
		max:    maxSize,
		empty:  sync.NewCond(locker),
		full:   sync.NewCond(locker),
		locker: locker,
		pos:    0,
		items:  make([]interface{}, 0, 128),
	}
}

func (queue *Queue) Push(item interface{}) {
	queue.locker.Lock()
	for len(queue.items) >= queue.max {
		queue.full.Wait()
	}
	queue.items = append(queue.items, item)
	queue.locker.Unlock()
	queue.empty.Signal()
}

func (queue *Queue) pushMany(items []interface{}) []interface{} {
	queue.locker.Lock()
	for len(queue.items) >= queue.max {
		queue.full.Wait()
	}
	remain := queue.max - len(queue.items)
	if len(items) < remain {
		remain = len(items)
	}
	queue.items = append(queue.items, items[:remain]...)
	queue.locker.Unlock()
	queue.empty.Signal()
	return items[remain:]
}

func (queue *Queue) PushMany(items []interface{}) {
	for len(items) != 0 {
		items = queue.pushMany(items)
	}
}

func (queue *Queue) Pop() interface{} {
	queue.locker.Lock()
	for len(queue.items) == 0 {
		queue.empty.Wait()
	}
	item := queue.items[0]
	queue.items[0] = nil
	queue.items = queue.items[1:]
	queue.pos++
	if queue.pos > 1024 {
		items := make([]interface{}, 0, len(queue.items))
		copy(items, queue.items)
		queue.items = items
		queue.pos = 0
	}
	queue.locker.Unlock()
	queue.full.Signal()
	return item
}

func (queue *Queue) PopAll(buf []interface{}) []interface{} {
	queue.locker.Lock()
	for len(queue.items) == 0 {
		queue.empty.Wait()
	}
	items := queue.items
	queue.items = buf[:0]
	queue.pos = 0
	queue.locker.Unlock()
	queue.full.Signal()
	return items
}

func (queue *Queue) PopAllWithoutBlock(buf []interface{}) []interface{} {
	queue.locker.Lock()
	if len(queue.items) == 0 {
		queue.locker.Unlock()
		return nil
	}
	items := queue.items
	queue.items = buf[:0]
	queue.pos = 0
	queue.locker.Unlock()
	queue.full.Signal()
	return items
}

var ErrQueueClose = errors.New("queue is closed")

type QueueWithContext struct {
	ctx      context.Context
	cancel   context.CancelFunc
	close    int32
	closeErr error
	locker   *sync.Mutex
	full     chan struct{}
	empty    chan struct{}
	pos      int
	max      int
	items    []interface{}
}

func NewQueueWithContext(ctx context.Context, cap int) *QueueWithContext {
	ctx, cancel := context.WithCancel(ctx)
	return &QueueWithContext{
		ctx:    ctx,
		cancel: cancel,
		locker: new(sync.Mutex),
		full:   make(chan struct{}, 1),
		empty:  make(chan struct{}, 1),
		pos:    0,
		close:  0,
		max:    cap,
		items:  make([]interface{}, 0, 64),
	}
}

func (queue *QueueWithContext) IsClose() bool {
	return atomic.LoadInt32(&queue.close) == 1
}

func (queue *QueueWithContext) fullWait() error {
	queue.locker.Unlock()
	select {
	case <-queue.full:
		queue.locker.Lock()
		return nil
	case <-queue.ctx.Done():
		queue.locker.Lock()
		return queue.ctx.Err()
	}
}

func (queue *QueueWithContext) emptySignal() {
	select {
	case queue.empty <- struct{}{}:
	default:
	}
}

func (queue *QueueWithContext) fullSignal() {
	select {
	case queue.full <- struct{}{}:
	default:
	}
}

func (queue *QueueWithContext) emptyWait() error {
	queue.locker.Unlock()
	select {
	case <-queue.empty:
		queue.locker.Lock()
		return nil
	case <-queue.ctx.Done():
		queue.locker.Lock()
		return queue.ctx.Err()
	}
}

func (queue *QueueWithContext) Push(item interface{}) error {
	queue.locker.Lock()
	for len(queue.items) >= queue.max {
		if err := queue.fullWait(); err != nil {
			queue.locker.Unlock()
			return err
		}
	}
	queue.items = append(queue.items, item)
	queue.locker.Unlock()
	queue.emptySignal()
	return nil
}

func (queue *QueueWithContext) pushMany(items []interface{}) ([]interface{}, error) {
	queue.locker.Lock()
	for len(queue.items) >= queue.max {
		if err := queue.fullWait(); err != nil {
			queue.locker.Unlock()
			return items, err
		}
	}
	remain := queue.max - len(queue.items)
	if len(items) < remain {
		remain = len(items)
	}
	queue.items = append(queue.items, items[:remain]...)
	queue.locker.Unlock()
	queue.emptySignal()
	return items[remain:], nil
}

func (queue *QueueWithContext) PushManyWithBlock(items []interface{}) ([]interface{}, error) {
	queue.locker.Lock()
	if len(queue.items) >= queue.max {
		queue.locker.Unlock()
		return items, nil
	}
	remain := queue.max - len(queue.items)
	if len(items) < remain {
		remain = len(items)
	}
	queue.items = append(queue.items, items[:remain]...)
	queue.locker.Unlock()
	queue.emptySignal()
	return items[remain:], nil
}

func (queue *QueueWithContext) PushMany(items []interface{}) error {
	var err error
	for len(items) != 0 {
		if items, err = queue.pushMany(items); err != nil {
			return err
		}
	}
	return nil
}

func (queue *QueueWithContext) Pop() (interface{}, error) {
	queue.locker.Lock()
	for len(queue.items) == 0 {
		if err := queue.emptyWait(); err != nil && len(queue.items) == 0{
			queue.locker.Unlock()
			return nil, err
		}
	}
	item := queue.items[0]
	queue.items[0] = nil
	queue.items = queue.items[1:]
	queue.pos++
	if queue.pos > 1024 {
		items := make([]interface{}, 0, len(queue.items))
		copy(items, queue.items)
		queue.items = items
		queue.pos = 0
	}
	queue.locker.Unlock()
	queue.fullSignal()
	return item, nil
}

func (queue *QueueWithContext) PopAll(buf []interface{}) ([]interface{}, error) {
	queue.locker.Lock()
	for len(queue.items) == 0 {
		if err := queue.emptyWait(); err != nil && len(queue.items) == 0{
			queue.locker.Unlock()
			return nil, err
		}
	}
	items := queue.items
	queue.items = buf[:0]
	queue.pos = 0
	queue.locker.Unlock()
	queue.fullSignal()
	return items, nil
}

func (queue *QueueWithContext) PopAllWithoutBlock(buf []interface{}) ([]interface{}, error) {
	queue.locker.Lock()
	if len(queue.items) == 0 {
		queue.locker.Unlock()
		return nil, nil
	}
	items := queue.items
	queue.items = buf[:0]
	queue.pos = 0
	queue.locker.Unlock()
	queue.fullSignal()
	return items, nil
}

func (queue *QueueWithContext) Ctx() context.Context {
	return queue.ctx
}

func (queue *QueueWithContext) Close(err error) {
	if atomic.CompareAndSwapInt32(&queue.close, 0, 1) {
		queue.closeErr = err
		queue.cancel()
	}
}

func (queue *QueueWithContext) CloseErr() error {
	return queue.closeErr
}
