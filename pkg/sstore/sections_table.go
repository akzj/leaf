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
	log "github.com/sirupsen/logrus"
	"sync"
)

type sectionsTable struct {
	l           sync.RWMutex
	endMap      *int64LockMap
	sectionsMap map[int64]*Sections
}

func newSectionsTable() *sectionsTable {
	return &sectionsTable{
		l:           sync.RWMutex{},
		sectionsMap: map[int64]*Sections{},
	}
}

func (table *sectionsTable) removeEmptySections(streamID int64) {
	table.l.Lock()
	defer table.l.Unlock()
	if sections, ok := table.sectionsMap[streamID]; ok {
		if _, ok := sections.begin(); ok == false {
			delete(table.sectionsMap, streamID)
		}
	}
}

func (table *sectionsTable) Get(streamID int64) *Sections {
	table.l.Lock()
	defer table.l.Unlock()
	if sections, ok := table.sectionsMap[streamID]; ok {
		return sections
	}
	return nil
}

func (table *sectionsTable) loadOrCreate(streamID int64, section Section) (*Sections, bool) {
	table.l.Lock()
	defer table.l.Unlock()
	if sections, ok := table.sectionsMap[streamID]; ok {
		return sections, true
	}
	sections := newSection(streamID, section)
	table.sectionsMap[streamID] = sections
	return sections, false
}

func (table *sectionsTable) UpdateSectionWithSegment(segment *segment) error {
	for _, it := range segment.meta.SectionOffsets {
		segment.IncRef()
		section := Section{
			segment: segment,
			stream:  nil,
			begin:   it.Begin,
			end:     it.End,
		}
		if sections, load := table.loadOrCreate(it.StreamID, section); load {
			if err := sections.insertOrUpdate(section); err != nil {
				return err
			}
		}
	}
	return nil
}

func (table *sectionsTable) removeSectionWithSegment(segment *segment) error {
	for _, info := range segment.meta.SectionOffsets {
		if sections := table.Get(info.StreamID); sections != nil {
			sections.remove(Section{
				segment: segment,
				stream:  nil,
				begin:   info.Begin,
				end:     info.End,
			})
			segment.DecRef()
			if sections.empty() {
				table.removeEmptySections(info.StreamID)
			}
		} else {
			return errors.Errorf("no find Sections for %d", info.StreamID)
		}
	}
	return nil
}

func (table *sectionsTable) UpdateSectionWithStream(stream *stream) {
	item := Section{
		segment: nil,
		stream:  stream,
		begin:   stream.begin,
		end:     stream.end,
	}
	if sections, loaded := table.loadOrCreate(stream.streamID, item); loaded {
		if err := sections.insertOrUpdate(item); err != nil {
			log.Panic(err)
		}
	}
}

func (table *sectionsTable) RemoveSectionWithStream(stream *stream) {
	if sections := table.Get(stream.streamID); sections != nil {
		sections.remove(Section{
			segment: nil,
			stream:  stream,
			begin:   stream.begin,
			end:     stream.end,
		})
		if sections.empty() {
			table.removeEmptySections(stream.streamID)
		}
	} else {
		panic("no find Sections")
	}
}

func (table *sectionsTable) Reader(streamID int64) (*sectionsReader, error) {
	sections := table.Get(streamID)
	if sections == nil {
		return nil, errors.Wrapf(ErrNoFindStream, "stream[%d]", streamID)
	}
	return newSectionsReader(streamID, sections, table.endMap), nil
}
