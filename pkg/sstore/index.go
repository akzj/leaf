package sstore

import (
	"github.com/pkg/errors"
	"log"
	"sort"
	"sync"
)

type offsetItem struct {
	segment *segment
	mStream *mStream
	begin   int64
	end     int64
}

type offsetIndex struct {
	streamID int64
	l        sync.RWMutex
	items    []offsetItem
}

var offsetIndexNoFind = offsetItem{}

func newOffsetIndex(streamID int64, item offsetItem) *offsetIndex {
	return &offsetIndex{
		streamID: streamID,
		l:        sync.RWMutex{},
		items:    append(make([]offsetItem, 0, 128), item),
	}
}

func (index *offsetIndex) find(offset int64) (offsetItem, error) {
	index.l.RLock()
	defer index.l.RUnlock()
	if len(index.items) == 0 {
		return offsetIndexNoFind, errors.WithStack(ErrNoFindOffsetIndex)
	}
	if index.items[len(index.items)-1].begin <= offset {
		return index.items[len(index.items)-1], nil
	}
	i := sort.Search(len(index.items), func(i int) bool {
		return index.items[i].begin > offset
	})
	return index.items[i-1], nil
}

func (index *offsetIndex) update(item offsetItem) error {
	index.l.Lock()
	defer index.l.Unlock()
	if len(index.items) == 0 {
		index.items = append(index.items, item)
		return nil
	}
	if index.items[len(index.items)-1].begin < item.begin {
		index.items[len(index.items)-1].end = item.begin
		index.items = append(index.items, item)
		return nil
	}
	i := sort.Search(len(index.items), func(i int) bool {
		return index.items[i].begin >= item.begin
	})
	if i < len(index.items) && index.items[i].begin == item.begin {
		index.items[i].end = item.end
		if item.segment != nil {
			if index.items[i].segment != nil {
				panic("segment not nil")
			}
			index.items[i].segment = item.segment
		}
		if item.mStream != nil {
			index.items[i].mStream = item.mStream
		}
		return nil
	} else {
		return errors.Errorf("update index error begin[%d] end[%d]",
			item.begin, item.end)
	}
}
func (index *offsetIndex) begin() (int64, bool) {
	index.l.RLock()
	defer index.l.RUnlock()
	if len(index.items) == 0 {
		return 0, false
	}
	return index.items[0].begin, true
}

func (index *offsetIndex) remove(item offsetItem) {
	index.l.Lock()
	defer index.l.Unlock()
	i := sort.Search(len(index.items), func(i int) bool {
		return index.items[i].begin >= item.begin
	})
	if i < len(index.items) && index.items[i].begin == item.begin {
		if item.segment != nil {
			if index.items[i].segment == nil {
				panic("segment null")
			}
			index.items[i].segment = nil
		}
		if item.mStream != nil {
			index.items[i].mStream = nil
		}
		if index.items[i].mStream == nil && index.items[i].segment == nil {
			copy(index.items[i:], index.items[i+1:])
			index.items[len(index.items)-1] = offsetItem{}
			index.items = index.items[:len(index.items)-1]
		}
	} else {
		panic("index remove error")
	}
}

type indexTable struct {
	l        sync.RWMutex
	endMap   *int64LockMap
	indexMap map[int64]*offsetIndex
}

func newIndexTable() *indexTable {
	return &indexTable{
		l:        sync.RWMutex{},
		indexMap: map[int64]*offsetIndex{},
	}
}

func (index *indexTable) removeEmptyOffsetIndex(streamID int64) {
	index.l.Lock()
	defer index.l.Unlock()
	if offsetIndex, ok := index.indexMap[streamID]; ok {
		if _, ok := offsetIndex.begin(); ok == false {
			delete(index.indexMap, streamID)
		}
	}
}

func (index *indexTable) get(streamID int64) *offsetIndex {
	index.l.Lock()
	defer index.l.Unlock()
	if offsetIndex, ok := index.indexMap[streamID]; ok {
		return offsetIndex
	}
	return nil
}

func (index *indexTable) loadOrCreate(streamID int64, item offsetItem) (*offsetIndex, bool) {
	index.l.Lock()
	defer index.l.Unlock()
	if offsetIndex, ok := index.indexMap[streamID]; ok {
		return offsetIndex, true
	}
	offsetIndex := newOffsetIndex(streamID, item)
	index.indexMap[streamID] = offsetIndex
	return offsetIndex, false
}

func (index *indexTable) update1(segment *segment) error {
	for _, it := range segment.meta.OffSetInfos {
		segment.refInc()
		item := offsetItem{
			segment: segment,
			mStream: nil,
			begin:   it.Begin,
			end:     it.End,
		}
		offsetIndex, load := index.loadOrCreate(it.StreamID, item)
		if load {
			if err := offsetIndex.update(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (index *indexTable) remove1(segment *segment) error {
	for _, info := range segment.meta.OffSetInfos {
		if offsetIndex := index.get(info.StreamID); offsetIndex != nil {
			offsetIndex.remove(offsetItem{
				segment: segment,
				mStream: nil,
				begin:   info.Begin,
				end:     info.End,
			})
			segment.refDec()
			if _, ok := offsetIndex.begin(); ok == false {
				index.removeEmptyOffsetIndex(info.StreamID)
			}
		} else {
			return errors.Errorf("no find offsetIndex for %d", info.StreamID)
		}
	}
	return nil
}

func (index *indexTable) update(stream *mStream) {
	item := offsetItem{
		segment: nil,
		mStream: stream,
		begin:   stream.begin,
		end:     stream.end,
	}
	offsetIndex, loaded := index.loadOrCreate(stream.streamID, item)
	if loaded {
		if err := offsetIndex.update(item); err != nil {
			log.Panicf("%+v", err)
		}
	}
}

func (index *indexTable) remove(stream *mStream) {
	if offsetIndex := index.get(stream.streamID); offsetIndex != nil {
		offsetIndex.remove(offsetItem{
			segment: nil,
			mStream: stream,
			begin:   stream.begin,
			end:     stream.end,
		})
		if _, ok := offsetIndex.begin(); ok == false {
			index.removeEmptyOffsetIndex(stream.streamID)
		}
	} else {
		panic("no find offsetIndex")
	}
}

func (index *indexTable) reader(streamID int64) (*reader, error) {
	offsetIndex := index.get(streamID)
	if offsetIndex == nil {
		return nil, errors.Wrapf(ErrNoFindStream, "stream[%d]", streamID)
	}
	return newReader(streamID, offsetIndex, index.endMap), nil
}
