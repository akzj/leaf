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
	"github.com/pkg/errors"
	"io"
	"sync"
)

//memory stream
type stream struct {
	locker   sync.RWMutex
	streamID int64
	begin    int64
	end      int64
	pageSize int
	Pages    []page
}

func newStream(offset int64, pageSize int, streamID int64) *stream {
	return &stream{
		locker:   sync.RWMutex{},
		streamID: streamID,
		begin:    offset,
		end:      offset,
		pageSize: pageSize,
		Pages:    append(make([]page, 0, 128), newPage(offset, pageSize)),
	}
}

func (m *stream) ReadAt(p []byte, off int64) (n int, err error) {
	m.locker.RLock()
	defer m.locker.RUnlock()
	if off < m.begin || off > m.end {
		return 0, errors.Wrapf(ErrOffset,
			"sectionOffset[%d] begin[%d] end[%d]", off, m.begin, m.end)
	}
	if off == m.end {
		return 0, io.EOF
	}
	offset := off - m.begin
	index := offset / int64(m.pageSize)
	offset = offset % int64(m.pageSize)

	var ret int
	for len(p) > 0 {
		block := &m.Pages[index]
		n := copy(p, block.buf[offset:block.limit])
		offset = 0
		ret += n
		p = p[n:]
		index++
		if index >= int64(len(m.Pages)) {
			break
		}
	}
	if ret == 0 {
		return 0, io.EOF
	}
	return ret, nil
}

func (m *stream) WriteAt(p []byte, offset int64, ) (int, error) {
	m.locker.Lock()
	defer m.locker.Unlock()
	if offset != -1 && m.end != offset {
		return 0, fmt.Errorf("sectionOffset error")
	}
	var size = len(p)
	for len(p) > 0 {
		if m.Pages[len(m.Pages)-1].limit == m.pageSize {
			m.Pages = append(m.Pages, newPage(m.end, m.pageSize))
		}
		block := &m.Pages[len(m.Pages)-1]
		n := copy(block.buf[block.limit:], p)
		block.limit += n
		m.end += int64(n)
		p = p[n:]
	}
	return size, nil
}

func (m *stream) WriteTo(writer io.Writer) (int64, error) {
	m.locker.RLock()
	defer m.locker.RUnlock()
	var n int64
	for i := range m.Pages {
		ret, err := (&m.Pages[i]).WriteTo(writer)
		n += ret
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

type page struct {
	limit int
	begin int64
	buf   []byte
}

func newPage(begin int64, blockSize int) page {
	return page{
		limit: 0,
		begin: begin,
		buf:   make([]byte, blockSize),
	}
}

func (p *page) WriteTo(writer io.Writer) (int64, error) {
	n, err := writer.Write(p.buf[:p.limit])
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return int64(n), err
}
