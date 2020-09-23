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
		assert.Equal(t, ints[i], s.expect, fmt.Sprintf("%d %d %d", s.find, s.expect, ints[i]))
	}
}

func TestSections(t *testing.T) {

	var streamID int64 = 1
	var sections *Sections
	t.Run("newSection", func(t *testing.T) {
		sections = newSection(streamID, Section{
			segment: &segment{},
			stream: &stream{
				streamID: streamID,
				begin:    0,
				end:      10,
			},
			begin: 1,
			end:   10,
		})
		assert.NotNil(t, sections)
	})

	var offsets = []struct {
		begin int64
		end   int64
	}{{10, 14}, {14, 777}, {777, 999}, {999, 1111}}

	t.Run("insertOrUpdate_segment", func(t *testing.T) {
		for _, offset := range offsets {
			assert.NoError(t, sections.insertOrUpdate(Section{
				segment: &segment{},
				begin:   offset.begin,
				end:     offset.end,
			}))
		}
	})

	t.Run("begin", func(t *testing.T) {
		begin, ok := sections.begin()
		assert.True(t, ok)
		assert.Equal(t, begin, int64(1))
	})

	t.Run("getSections", func(t *testing.T) {
		items := sections.getSections()
		assert.Equal(t, len(items), 5)
	})

	t.Run("find_segment", func(t *testing.T) {
		for i := int64(1); i <= offsets[len(offsets)-1].end; i++ {
			item, _, err := sections.Find(i)
			assert.NoError(t, err)
			assert.NotNil(t, item.segment)
			assert.True(t, item.begin <= i && i <= item.end, fmt.Sprintf("%d %d %d", item.begin, i, item.end))
		}
	})

	t.Run("insertOrUpdate_mStream", func(t *testing.T) {
		for _, offset := range offsets {
			assert.NoError(t, sections.insertOrUpdate(Section{
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
			item, _, err := sections.Find(i)
			assert.NoError(t, err)
			assert.NotNil(t, item.segment)
			assert.NotNil(t, item.stream)
			assert.True(t, item.begin <= i && i <= item.end, fmt.Sprintf("%d %d %d", item.begin, i, item.end))
		}
	})

	t.Run("remove_segment", func(t *testing.T) {
		for _, offset := range offsets {
			sections.remove(Section{
				segment: &segment{},
				stream:  nil,
				begin:   offset.begin,
				end:     offset.end,
			})
			item, _, err := sections.Find(offset.begin)
			assert.NoError(t, err)
			assert.NotNil(t, item.stream)
			assert.Nil(t, item.segment)
		}
	})

	t.Run("Find", func(t *testing.T) {
		for _, offset := range offsets {
			sections.remove(Section{
				stream: &stream{
					begin: offset.begin,
					end:   offset.end,
				},
				begin: offset.begin,
				end:   offset.end,
			})
			item, _, err := sections.Find(offset.begin)
			if err != nil {
				assert.NotEqual(t, item.begin, offset.begin)
			}
		}
	})
}
