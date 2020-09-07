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
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
)

const version1 = "ver1"

type JournalMeta struct {
	Old          bool   `json:"old"`
	Filename     string `json:"filename"`
	Version      string `json:"version"`
	FirstEntryID int64  `json:"first_entry_id"`
	LastEntryID  int64  `json:"last_entry_id"`
}

// write ahead log
type journal struct {
	filename string
	size     int64
	f        *os.File
	writer   *bufio.Writer
	meta     JournalMeta
}

func openJournal(filename string) (*journal, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_CREATE, 0666)
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
		meta: JournalMeta{
			Filename:     filepath.Base(filename),
			Version:      version1,
			FirstEntryID: -1,
			LastEntryID:  -1,
			Old:          false,
		},
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

func (j *journal) GetMeta() JournalMeta {
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

func (j *journal) Write(e *entry) error {
	j.meta.LastEntryID = e.ID
	if j.meta.FirstEntryID == -1 {
		j.meta.FirstEntryID = e.ID
	}
	if err := e.write(j.writer); err != nil {
		return err
	}
	j.size += int64(e.size())
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

func (j *journal) Read(cb func(e *entry) error) error {
	reader := bufio.NewReader(j.f)
	for {
		e, err := decodeEntry(reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := cb(e); err != nil {
			return err
		}
	}
}
