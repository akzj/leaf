package sstore

import (
	"fmt"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	"io"
)

type segmentReader struct {
	indexInfo *pb.OffsetInfo
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
