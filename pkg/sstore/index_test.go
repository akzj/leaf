package sstore

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestSortSearch(t *testing.T) {

	cases := []struct {
		find   int
		expect int
	}{
		{
			0,
			1,
		},
		{
			1,
			3,
		},
		{
			find:   2,
			expect: 3,
		},
		{
			find:   3,
			expect: 4,
		},
		{
			find:   5,
			expect: 7,
		},
		{
			find:   6,
			expect: 7,
		},
		{
			find:   7,
			expect: 9,
		},
		{
			find:   9,
			expect: 11,
		},
	}

	ints := []int{1, 3, 4, 7, 9, 11}
	for _, s := range cases {
		i := sort.Search(len(ints), func(i int) bool {
			return s.find < ints[i]
		})
		assert.Equal(t, ints[i], s.expect,fmt.Sprintf("%d %d %d",s.find,s.expect,ints[i]))
	}
}

func TestOffsetIndex(t *testing.T) {

	var streamID int64 = 1
	var index *offsetIndex
	t.Run("newOffsetIndex", func(t *testing.T) {
		index = newOffsetIndex(streamID, offsetItem{
			segment: &segment{},
			stream: &stream{
				streamID: streamID,
				begin:    0,
				end:      10,
			},
			begin: 1,
			end:   10,
		})
		assert.NotNil(t, index)
	})

	var offsets = []struct {
		begin int64
		end   int64
	}{{10, 14}, {14, 777}, {777, 999}, {999, 1111}}

	t.Run("insertOrUpdate_segment", func(t *testing.T) {
		for _, offset := range offsets {
			assert.NoError(t, index.insertOrUpdate(offsetItem{
				segment: &segment{},
				begin:   offset.begin,
				end:     offset.end,
			}))
		}
	})

	t.Run("begin", func(t *testing.T) {
		begin, ok := index.begin()
		assert.True(t, ok)
		assert.Equal(t, begin, int64(1))
	})

	t.Run("getItems", func(t *testing.T) {
		items := index.getItems()
		assert.Equal(t, len(items), 5)
	})

	t.Run("find_segment", func(t *testing.T) {
		for i := int64(1); i <= offsets[len(offsets)-1].end; i++ {
			item, err := index.find(i)
			assert.NoError(t, err)
			assert.NotNil(t, item.segment)
			assert.True(t, item.begin <= i && i <= item.end, fmt.Sprintf("%d %d %d", item.begin, i, item.end))
		}
	})

	t.Run("insertOrUpdate_mStream", func(t *testing.T) {
		for _, offset := range offsets {
			assert.NoError(t, index.insertOrUpdate(offsetItem{
				stream: &stream{
					streamID: streamID,
					begin:    offset.begin,
					end:      offset.end,
				},
				begin: offset.begin,
				end:   offset.end,
			}))
		}
	})

	t.Run("find_m_stream", func(t *testing.T) {
		for i := int64(1); i <= offsets[len(offsets)-1].end; i++ {
			item, err := index.find(i)
			assert.NoError(t, err)
			assert.NotNil(t, item.segment)
			assert.NotNil(t, item.stream)
			assert.True(t, item.begin <= i && i <= item.end, fmt.Sprintf("%d %d %d", item.begin, i, item.end))
		}
	})

	t.Run("remove_segment", func(t *testing.T) {
		for _, offset := range offsets {
			index.remove(offsetItem{
				segment: &segment{},
				stream:  nil,
				begin:   offset.begin,
				end:     offset.end,
			})
			item, err := index.find(offset.begin)
			assert.NoError(t, err)
			assert.NotNil(t, item.stream)
			assert.Nil(t, item.segment)
		}
	})

	t.Run("", func(t *testing.T) {
		for _, offset := range offsets {
			index.remove(offsetItem{
				stream: &stream{
					begin: offset.begin,
					end:   offset.end,
				},
				begin: offset.begin,
				end:   offset.end,
			})
			item, err := index.find(offset.begin)
			if err != nil {
				assert.NotEqual(t, item.begin, offset.begin)
			}
		}
	})
}
