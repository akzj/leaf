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
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/akzj/streamIO/pkg/utils"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type segment struct {
	*utils.RefCount
	filename     string
	f            *os.File
	meta         *pb.SegmentMeta
	l            *sync.RWMutex
	delete       bool
}

func newSegment() *segment {
	sm := &segment{
		filename: "",
		f:        nil,
		meta: &pb.SegmentMeta{
			From:        &pb.Version{},
			To:          &pb.Version{},
			CreateTS:    0,
			OffSetInfos: map[int64]*pb.OffsetInfo{},
		},
		l:            new(sync.RWMutex),
		delete:       false,
	}
	sm.RefCount = utils.NewRefCount(0, func() {
		_ = sm.close()
	})
	return sm
}

func createSegment(filename string) (*segment, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	segment := newSegment()
	segment.f = f
	segment.filename = filename
	return segment, nil
}

func openSegment(filename string) (*segment, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()
	segment := newSegment()
	segment.filename = filename
	segment.f = f
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
	if err := proto.Unmarshal(data, segment.meta); err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}
	f = nil
	return segment, nil
}
func (s *segment) FromVersion() *pb.Version {
	return s.meta.From
}


func (s *segment) offsetInfo(streamID int64) (*pb.OffsetInfo, error) {
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
		r:         NewSectionReader(s.f, info.Offset, info.End-info.Begin),
	}
}

func (s *segment) flushMStreamTable(table *streamTable) error {
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
		s.meta.OffSetInfos[streamID] = &pb.OffsetInfo{
			StreamID: streamID,
			Offset:   Offset,
			CRC:      hash.Sum32(),
			Begin:    mStream.begin,
			End:      mStream.end,
		}
		Offset += int64(n)
	}
	s.meta.From = table.from
	s.meta.To = table.to
	s.meta.CreateTS = table.CreateTS.Unix()
	data, err := proto.Marshal(s.meta)
	if err != nil {
		return err
	}
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

// NewSectionReader returns a SectionReader that reads from r
// starting at offset off and stops with EOF after n bytes.
func NewSectionReader(r io.ReaderAt, off int64, n int64) *SectionReader {
	return &SectionReader{r, off, off, off + n}
}

// SectionReader implements Read, Seek, and ReadAt on a section
// of an underlying ReaderAt.
type SectionReader struct {
	r     io.ReaderAt
	base  int64
	off   int64
	limit int64
}

func (s *SectionReader) Read(p []byte) (n int, err error) {
	if s.off >= s.limit {
		return 0, io.EOF
	}
	if max := s.limit - s.off; int64(len(p)) > max {
		p = p[0:max]
	}
	n, err = s.r.ReadAt(p, s.off)
	s.off += int64(n)
	return
}

var errWhence = errors.New("Seek: invalid whence")
var errOffset = errors.New("Seek: invalid offset")

func (s *SectionReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	default:
		return 0, errWhence
	case io.SeekStart:
		offset += s.base
	case io.SeekCurrent:
		offset += s.off
	case io.SeekEnd:
		offset += s.limit
	}
	if offset < s.base {
		return 0, errOffset
	}
	s.off = offset
	return offset - s.base, nil
}

func (s *SectionReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= s.limit-s.base {
		return 0, io.EOF
	}
	off += s.base
	if max := s.limit - off; int64(len(p)) > max {
		p = p[0:max]
		n, err = s.r.ReadAt(p, off)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}
	return s.r.ReadAt(p, off)
}

// Size returns the end of the section in bytes.
func (s *SectionReader) Size() int64 { return s.limit - s.base }

type segmentReader struct {
	indexInfo *pb.OffsetInfo
	r         *SectionReader
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
			fmt.Sprintf("offset[%d] begin[%d] end[%d]", offset, s.indexInfo.Begin, s.indexInfo.End))
	}
	size := s.indexInfo.End - offset
	if int64(len(p)) > size {
		p = p[:size]
	}
	offset = offset - s.indexInfo.Begin
	n, err = s.r.ReadAt(p, offset)
	if err == io.EOF {
		if n != 0 {
			return n, nil
		}
	}
	return n, err
}

func (s *segmentReader) Read(p []byte) (n int, err error) {
	return s.r.Read(p)
}
