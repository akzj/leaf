package sstore

import (
	"sync"
)

type endWatcher struct {
	index  int64
	c      func()
	int64s chan int64
}

type notify struct {
	streamID int64
	end      int64
}

type endWatchers struct {
	watchIndex     int64
	endWatcherMap  map[int64][]endWatcher
	endWatcherLock *sync.RWMutex

	l           *sync.Mutex
	cond        *sync.Cond
	notifyItems []*notify
	s           chan interface{}
}

var notifyPool = sync.Pool{New: func() interface{} {
	return new(notify)
}}

func newEndWatchers() *endWatchers {
	l := new(sync.Mutex)
	return &endWatchers{
		watchIndex:     0,
		l:              l,
		cond:           sync.NewCond(l),
		endWatcherLock: new(sync.RWMutex),
		notifyItems:    make([]*notify, 0, 1024),
		endWatcherMap:  make(map[int64][]endWatcher),
		s:              make(chan interface{}, 1),
	}
}
func (endWatchers *endWatchers) removeEndWatcher(index int64, streamID int64) {
	endWatchers.endWatcherLock.Lock()
	defer endWatchers.endWatcherLock.Unlock()
	endWatcherS, ok := endWatchers.endWatcherMap[streamID]
	if ok == false {
		return
	}
	for i, watcher := range endWatcherS {
		if watcher.index == index {
			copy(endWatcherS[i:], endWatcherS[i+1:])
			endWatcherS[len(endWatcherS)-1] = endWatcher{}
			endWatcherS = endWatcherS[:len(endWatcherS)-1]
			endWatchers.endWatcherMap[streamID] = endWatcherS
			break
		}
	}
}

func (endWatchers *endWatchers) newEndWatcher(streamID int64) *endWatcher {
	endWatchers.endWatcherLock.Lock()
	defer endWatchers.endWatcherLock.Unlock()
	endWatchers.watchIndex++
	index := endWatchers.watchIndex
	watcher := endWatcher{
		index:  index,
		int64s: make(chan int64, 1),
		c: func() {
			endWatchers.removeEndWatcher(index, streamID)
		},
	}
	endWatchers.endWatcherMap[streamID] = append(endWatchers.endWatcherMap[streamID], watcher)
	return &watcher
}

func (endWatchers *endWatchers) getEndWatcher(streamID int64) []endWatcher {
	endWatchers.endWatcherLock.RLock()
	watcher, _ := endWatchers.endWatcherMap[streamID]
	endWatchers.endWatcherLock.RUnlock()
	return watcher
}

func (endWatchers *endWatchers) take(buf []*notify) []*notify {
	endWatchers.l.Lock()
	defer endWatchers.l.Unlock()
	for len(endWatchers.notifyItems) == 0 {
		endWatchers.cond.Wait()
	}
	notifyItems := endWatchers.notifyItems
	endWatchers.notifyItems = buf[:0]
	return notifyItems
}

func (endWatchers *endWatchers) start() {
	go func() {
		var buf = make([]*notify, 0, 1024)
		for {
			items := endWatchers.take(buf)
			buf = items

			for _, item := range items {
				if item.end == closeSignal {
					close(endWatchers.s)
					return
				}
				for _, watcher := range endWatchers.getEndWatcher(item.streamID) {
					watcher.notify(item.end)
				}
				notifyPool.Put(item)
			}
		}
	}()
}

func (endWatchers *endWatchers) close() {
	endWatchers.notify(&notify{end: closeSignal})
	<-endWatchers.s
}

func (endWatchers *endWatchers) notify(item *notify) {
	endWatchers.l.Lock()
	endWatchers.notifyItems = append(endWatchers.notifyItems, item)
	endWatchers.l.Unlock()
	endWatchers.cond.Signal()
}

func (watcher *endWatcher) notify(pos int64) {
	select {
	case watcher.int64s <- pos:
	default:
	}
}

func (watcher *endWatcher) Watch() chan int64 {
	return watcher.int64s
}

func (watcher *endWatcher) Close() {
	if watcher.c != nil {
		watcher.c()
		watcher.c = nil
	}
}
