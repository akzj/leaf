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
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
)

const version1 = "ver1"

// write ahead log
type journal struct {
	filename     string
	size         int64
	f            *os.File
	writer       *bufio.Writer
	meta         *pb.JournalMeta
	index        *journalIndex
	offsetReader *offsetReader
	*ref
}

func openJournal(filename string) (*journal, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	w := &journal{
		filename: filename,
		size:     0,
		f:        f,
		writer:   bufio.NewWriterSize(f, 4*1024*1024),
		meta: &pb.JournalMeta{
			Old:      false,
			Filename: filepath.Base(filename),
			Version:  version1,
		},
		index: new(journalIndex),
		ref: newRef(1, func() {
		}),
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

func (j *journal) GetMeta() *pb.JournalMeta {
	return j.meta
}

func (j *journal) Flush() error {
	if err := j.writer.Flush(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (j *journal) Sync() error {
	if err := j.f.Sync(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (j *journal) Write(e *writeRequest) error {
	j.meta.To = e.entry.Ver
	if j.meta.From == nil {
		j.meta.From = e.entry.Ver
	}
	var offset = j.size
	if n, err := e.WriteTo(j.writer); err != nil {
		return err
	} else {
		j.size += n
	}
	j.index.append(jIndex{
		Offset: offset,
		Index:  e.entry.Ver.Index,
	})
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
	return nil
}

func (j *journal) Filename() string {
	return j.filename
}

func (j *journal) Read(cb func(e *writeRequest) error) error {
	j.offsetReader = &offsetReader{
		reader: bufio.NewReader(j.f),
		offset: 0,
	}
	for {
		e, err := decodeEntry(j.offsetReader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.WithStack(err)
		}
		if err := cb(&writeRequest{entry: e}); err != nil {
			return err
		}
	}
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

func (j *journal) RebuildIndex() error {
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	j.index = new(journalIndex)
	var offset int64
	return j.Read(func(e *writeRequest) error {
		j.index.append(jIndex{
			Offset: offset,
			Index:  e.entry.Ver.Index,
		})
		offset = j.offsetReader.offset
		return nil
	})
}
