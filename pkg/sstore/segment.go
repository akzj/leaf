// Copyright 2020-2026 The sstore Authors
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
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"sync"
	"time"
)

type offsetInfo struct {
	StreamID int64  `json:"stream_id"`
	Begin    int64  `json:"begin"`
	Offset   int64  `json:"offset"`
	End      int64  `json:"end"`
	CRC      uint32 `json:"crc"`
}

type segmentMeta struct {
	Ver         Version               `json:"ver"`
	GcTS        time.Time             `json:"gc_ts"`
	LastEntryID int64                 `json:"last_entry_id"`
	OffSetInfos map[int64]offsetInfo `json:"offset_infos"`
}

type segment struct {
	*ref
	filename string
	f        *os.File
	meta     *segmentMeta
	l        *sync.RWMutex
	delete   bool
}

func createSegment(filename string) (*segment, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	segment := &segment{
		f:        f,
		filename: filename,
		meta:     new(segmentMeta),
		l:        new(sync.RWMutex),
	}
	segment.ref = newRef(0, func() {
		if err := segment.close(); err != nil {
			log.Fatal(err.Error())
		}
	})
	segment.meta.OffSetInfos = make(map[int64]offsetInfo)
	return segment, nil
}

func openSegment(filename string) (*segment, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	segment := &segment{
		ref:      nil,
		filename: filename,
		f:        f,
		meta:     new(segmentMeta),
		l:        new(sync.RWMutex),
		delete:   false,
	}
	//seek to Read meta length
	if _, err := f.Seek(-4, io.SeekEnd); err != nil {
		return nil, errors.WithStack(err)
	}
	//Read meta length
	var headerLen int32
	if err := binary.Read(f, binary.BigEndian, &headerLen); err != nil {
		return nil, errors.WithStack(err)
	}
	//seek to Read meta
	if _, err := f.Seek(-int64(headerLen)-4, io.SeekEnd); err != nil {
		return nil, errors.WithStack(err)
	}
	data := make([]byte, headerLen)
	n, err := f.Read(data)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if n != len(data) {
		return nil, errors.WithMessage(io.ErrUnexpectedEOF, "Read segment head failed")
	}
	if err := json.Unmarshal(data, &segment.meta); err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}
	segment.ref = newRef(0, func() {
		if err := segment.close(); err != nil {
			log.Fatal(err.Error())
		}
	})
	return segment, nil
}
func (s *segment) lastEntryID() int64 {
	return s.meta.LastEntryID
}
func (s *segment) offsetInfo(streamID int64) (offsetInfo, error) {
	indexInfo, ok := s.meta.OffSetInfos[streamID]
	if ok == false {
		return indexInfo, ErrNoFindIndexInfo
	}
	return indexInfo, nil
}

func (s *segment) Reader(streamID int64) *segmentReader {
	info, ok := s.meta.OffSetInfos[streamID]
	if !ok {
		return nil
	}
	return &segmentReader{
		indexInfo: info,
		r:         io.NewSectionReader(s.f, info.Offset, info.End),
	}
}

func (s *segment) flushMStreamTable(table *mStreamTable) error {
	s.l.Lock()
	defer s.l.Unlock()
	var Offset int64
	writer := bufio.NewWriterSize(s.f, 1024*1024)
	for streamID, mStream := range table.mStreams {
		hash := crc32.NewIEEE()
		mWriter := io.MultiWriter(writer, hash)
		n, err := mStream.writeTo(mWriter)
		if err != nil {
			return err
		}
		index := offsetInfo{
			StreamID: streamID,
			Offset:   Offset,
			CRC:      hash.Sum32(),
			Begin:    mStream.begin,
			End:      mStream.end,
		}
		Offset += int64(n)
		s.meta.OffSetInfos[streamID] = index
	}
	s.meta.LastEntryID = table.lastEntryID
	s.meta.GcTS = table.GcTS
	data, _ := json.Marshal(s.meta)
	if _, err := writer.Write(data); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, int32(len(data))); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return nil
}

func (s *segment) deleteOnClose(delete bool) error {
	s.l.Lock()
	defer s.l.Unlock()
	if s.f == nil {
		return errors.Errorf("segment is close")
	}
	s.delete = delete
	return nil
}

func (s *segment) close() error {
	s.l.Lock()
	defer s.l.Unlock()
	if s.f == nil {
		return nil
	}
	if err := s.f.Close(); err != nil {
		return err
	}
	if s.delete {
		if err := os.Remove(s.filename); err != nil {
			return errors.WithStack(err)
		}
	}
	s.f = nil
	return nil
}

type segmentReader struct {
	indexInfo offsetInfo
	r         *io.SectionReader
}

func (s *segmentReader) Seek(offset int64, whence int) (int64, error) {
	if offset < s.indexInfo.Begin || offset >= s.indexInfo.End {
		return 0, ErrOffset
	}
	offset = offset - s.indexInfo.Begin
	return s.r.Seek(offset, whence)
}

func (s *segmentReader) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset < s.indexInfo.Begin || offset >= s.indexInfo.End {
		return 0, errors.Wrapf(ErrOffset,
			fmt.Sprintf("offset[%d] begin[%d] end[%d]",
				offset, s.indexInfo.Begin, s.indexInfo.End))
	}
	size := s.indexInfo.End - offset
	if int64(len(p)) > size {
		p = p[:size]
	}
	offset = offset - s.indexInfo.Begin
	return s.r.ReadAt(p, offset)
}

func (s *segmentReader) Read(p []byte) (n int, err error) {
	return s.r.Read(p)
}

type ref struct {
	l sync.RWMutex
	c int32
	f func()
}

func newRef(count int32, f func()) *ref {
	return &ref{
		c: count,
		f: f,
	}
}

func (ref *ref) refCount() int32 {
	ref.l.Lock()
	c := ref.c
	ref.l.Unlock()
	return c
}

func (ref *ref) refDec() int32 {
	ref.l.Lock()
	defer ref.l.Unlock()
	if ref.c <= 0 {
		panic(fmt.Errorf("ref.c %d error", ref.c))
	}
	ref.c -= 1
	if ref.c == 0 {
		ref.c = math.MinInt32
		go ref.f()
		return 0
	}
	return ref.c
}

func (ref *ref) refInc() int32 {
	ref.l.Lock()
	defer ref.l.Unlock()
	ref.c += 1
	return ref.c
}
