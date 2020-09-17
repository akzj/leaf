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
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/edsrzf/mmap-go"
	pproto "github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"unsafe"
)

const version1 = "ver1"

// write ahead log
type journal struct {
	*ref
	filename string
	size     int64
	f        *os.File
	writer   *bufio.Writer
	meta     *pb.JournalMeta
	index    *journalIndex
	flushVer unsafe.Pointer //*pb.Version

	JournalMMap *JournalMMap
}

type JournalMMap struct {
	*ref
	data mmap.MMap
}

const mmapSize = 1024 * 1024 * 1024 * 1024

func openJournalMMap(f *os.File) *JournalMMap {
	m, err := mmap.MapRegion(f, mmapSize, mmap.RDONLY, 0, 0)
	if err != nil {
		panic(err)
	}
	jmmap := JournalMMap{
		ref:  nil,
		data: m[:mmapSize],
	}
	jmmap.ref = newRef(1, func() {
		if err := jmmap.data.Unmap(); err != nil {
			panic(err)
		}
	})
	return &jmmap
}

func OpenJournal(filename string) (*journal, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var w = &journal{
		ref: newRef(1, func() {
		}),
		filename: filename,
		size:     stat.Size(),
		f:        f,
		writer:   bufio.NewWriterSize(f, 4*1024*1024),
		meta: &pb.JournalMeta{
			Old:      false,
			Filename: filepath.Base(filename),
			From:     &pb.Version{},
			To:       &pb.Version{},
		},
		index:       new(journalIndex),
		flushVer:    unsafe.Pointer(&pb.Version{}),
		JournalMMap: openJournalMMap(f),
	}
	return w, nil
}

func (j *journal) SeekStart() error {
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (j *journal) SeekEnd() error {
	if _, err := j.f.Seek(0, io.SeekEnd); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (j *journal) SetMeta(meta *pb.JournalMeta) {
	j.meta = meta
}

func (j *journal) GetMeta() *pb.JournalMeta {
	return j.meta
}

func (j *journal) Flush() error {
	if err := j.writer.Flush(); err != nil {
		return errors.WithStack(err)
	}
	atomic.StorePointer(&j.flushVer, unsafe.Pointer(j.meta.To))
	return nil
}

func (j *journal) GetFlushIndex() int64 {
	return (*pb.Version)(atomic.LoadPointer(&j.flushVer)).Index
}

func (j *journal) Sync() error {
	if err := j.f.Sync(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (j *journal) Write(entry *pb.Entry) error {
	j.meta.To = entry.Ver
	if j.meta.From.Index == 0 {
		j.meta.From = entry.Ver
	}
	data, err := pproto.Marshal(entry)
	if err != nil {
		return err
	}
	if err := binary.Write(j.writer, binary.BigEndian, int32(len(data))); err != nil {
		return err
	}
	if _, err := j.writer.Write(data); err != nil {
		return err
	}
	j.index.append(jIndex{
		Offset: j.size,
		Index:  entry.Ver.Index,
	})
	j.size += int64(len(data) + 4)
	return nil
}

func (j *journal) Size() int64 {
	return j.size
}

func (j *journal) Close() error {
	if err := j.Flush(); err != nil {
		return errors.WithStack(err)
	}
	if err := j.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	j.JournalMMap.refDec()
	return nil
}
func (j *journal) GetJournalMMap() *JournalMMap {
	if count := j.JournalMMap.refInc(); count < 0 {
		return nil
	}
	return j.JournalMMap
}

func (j *journal) Filename() string {
	return j.filename
}

type offsetReader struct {
	reader io.Reader
	offset int64
}

func (o *offsetReader) Read(p []byte) (n int, err error) {
	n, err = o.reader.Read(p)
	o.offset += int64(n)
	return n, err
}

func (j *journal) Range(callback func(entry *pb.Entry) error) error {
	j.index = new(journalIndex)
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var offsetReader = &offsetReader{
		reader: bufio.NewReader(j.f),
		offset: 0,
	}
	for {
		var offset = offsetReader.offset
		entry, err := DecodeEntry(offsetReader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.WithStack(err)
		}
		j.meta.To = entry.Ver
		j.flushVer = unsafe.Pointer(entry.Ver)
		if j.meta.From.Index == 0 {
			j.meta.From = entry.Ver
		}
		j.index.append(jIndex{
			Offset: offset,
			Index:  entry.Ver.Index,
		})
		if err := callback(entry); err != nil {
			return err
		}
	}
}

func (j *journal) Index() *journalIndex {
	return j.index
}

func DecodeEntry(reader io.Reader) (*pb.Entry, error) {
	var size int32
	if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}
	var entry pb.Entry
	if err := pproto.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}
