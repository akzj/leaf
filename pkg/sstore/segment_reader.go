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
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	"io"
)

type segmentReader struct {
	sectionOffset *pb.SectionOffset
	r             *io.SectionReader
}

func (s *segmentReader) Seek(offset int64, whence int) (int64, error) {
	if offset < s.sectionOffset.Begin || offset >= s.sectionOffset.End {
		return 0, ErrOffset
	}
	offset = offset - s.sectionOffset.Begin
	return s.r.Seek(offset, whence)
}

func (s *segmentReader) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset < s.sectionOffset.Begin || offset >= s.sectionOffset.End {
		return 0, errors.Wrapf(ErrOffset, fmt.Sprintf("offset %d sectionOffset[%d,%d)",
			offset, s.sectionOffset.Begin, s.sectionOffset.End))
	}
	size := s.sectionOffset.End - offset
	if int64(len(p)) > size {
		p = p[:size]
	}
	offset = offset - s.sectionOffset.Begin
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
