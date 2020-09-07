package block_queue

import "sync"

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
