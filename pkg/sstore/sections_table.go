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

func (table *sectionsTable) get(streamID int64) *Sections {
	table.l.Lock()
	defer table.l.Unlock()
	if sections, ok := table.sectionsMap[streamID]; ok {
		return sections
	}
	return nil
}

func (table *sectionsTable) loadOrCreate(streamID int64, item Section) (*Sections, bool) {
	table.l.Lock()
	defer table.l.Unlock()
	if sections, ok := table.sectionsMap[streamID]; ok {
		return sections, true
	}
	section := newSection(streamID, item)
	table.sectionsMap[streamID] = section
	return section, false
}

func (table *sectionsTable) update1(segment *segment) error {
	for _, it := range segment.meta.OffSetInfos {
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

func (table *sectionsTable) remove1(segment *segment) error {
	for _, info := range segment.meta.OffSetInfos {
		if sections := table.get(info.StreamID); sections != nil {
			sections.remove(Section{
				segment: segment,
				stream:  nil,
				begin:   info.Begin,
				end:     info.End,
			})
			segment.DecRef()
			if _, ok := sections.begin(); ok == false {
				table.removeEmptySections(info.StreamID)
			}
		} else {
			return errors.Errorf("no find Sections for %d", info.StreamID)
		}
	}
	return nil
}

func (table *sectionsTable) update(stream *stream) {
	item := Section{
		segment: nil,
		stream:  stream,
		begin:   stream.begin,
		end:     stream.end,
	}
	sections, loaded := table.loadOrCreate(stream.streamID, item)
	if loaded {
		if err := sections.insertOrUpdate(item); err != nil {
			log.Panicf("%+v", err)
		}
	}
}

func (table *sectionsTable) remove(stream *stream) {
	if sections := table.get(stream.streamID); sections != nil {
		sections.remove(Section{
			segment: nil,
			stream:  stream,
			begin:   stream.begin,
			end:     stream.end,
		})
		if _, ok := sections.begin(); ok == false {
			table.removeEmptySections(stream.streamID)
		}
	} else {
		panic("no find Sections")
	}
}

func (table *sectionsTable) reader(streamID int64) (*reader, error) {
	sections := table.get(streamID)
	if sections == nil {
		return nil, errors.Wrapf(ErrNoFindStream, "stream[%d]", streamID)
	}
	return newReader(streamID, sections, table.endMap), nil
}
