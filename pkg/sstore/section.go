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
	"github.com/pkg/errors"
	"sort"
	"sync"
)

type Section struct {
	segment *segment
	stream  *stream
	//[begin,end) exclude end
	begin int64
	end   int64
}

type Sections struct {
	streamID int64
	l        sync.RWMutex
	sections []Section
}

var streamSectionNoFind = Section{}

func newSection(streamID int64, section Section) *Sections {
	return &Sections{
		streamID: streamID,
		l:        sync.RWMutex{},
		sections: append(make([]Section, 0, 128), section),
	}
}
func (sections *Sections) getSections() []Section {
	sections.l.RLock()
	defer sections.l.RUnlock()
	return append(make([]Section, 0, len(sections.sections)), sections.sections...)
}

func (sections *Sections) find(offset int64) (Section, error) {
	sections.l.RLock()
	defer sections.l.RUnlock()
	if len(sections.sections) == 0 {
		return streamSectionNoFind, errors.WithStack(ErrNoFindSection)
	}
	if sections.sections[len(sections.sections)-1].end < offset {
		return sections.sections[len(sections.sections)-1], nil
	}
	i := sort.Search(len(sections.sections), func(i int) bool {
		return offset < sections.sections[i].end
	})
	if i < len(sections.sections) {
		return sections.sections[i], nil
	}
	return sections.sections[i-1], nil
}

func (sections *Sections) insertOrUpdate(section Section) error {
	sections.l.Lock()
	defer sections.l.Unlock()
	if len(sections.sections) == 0 {
		sections.sections = append(sections.sections, section)
		return nil
	}
	if sections.sections[len(sections.sections)-1].begin < section.begin {
		sections.sections[len(sections.sections)-1].end = section.begin
		sections.sections = append(sections.sections, section)
		return nil
	}
	i := sort.Search(len(sections.sections), func(i int) bool {
		return section.begin <= sections.sections[i].begin
	})
	if i < len(sections.sections) && sections.sections[i].begin == section.begin {
		sections.sections[i].end = section.end
		if section.segment != nil {
			if sections.sections[i].segment != nil {
				panic("segment not nil")
			}
			sections.sections[i].segment = section.segment
		}
		if section.stream != nil {
			sections.sections[i].stream = section.stream
		}
		return nil
	} else {
		return errors.Errorf("insertOrUpdate sections error begin[%d] end[%d]",
			section.begin, section.end)
	}
}
func (sections *Sections) begin() (int64, bool) {
	sections.l.RLock()
	defer sections.l.RUnlock()
	if len(sections.sections) == 0 {
		return 0, false
	}
	return sections.sections[0].begin, true
}

func (sections *Sections) remove(section Section) {
	sections.l.Lock()
	defer sections.l.Unlock()
	i := sort.Search(len(sections.sections), func(i int) bool {
		return section.begin <= sections.sections[i].begin
	})
	if i < len(sections.sections) && sections.sections[i].begin == section.begin {
		if section.segment != nil {
			if sections.sections[i].segment == nil {
				panic("segment null")
			}
			sections.sections[i].segment = nil
		}
		if section.stream != nil {
			sections.sections[i].stream = nil
		}
		if sections.sections[i].stream == nil && sections.sections[i].segment == nil {
			copy(sections.sections[i:], sections.sections[i+1:])
			sections.sections[len(sections.sections)-1] = Section{}
			sections.sections = sections.sections[:len(sections.sections)-1]
		}
	} else {
		panic("sections remove error")
	}
}


