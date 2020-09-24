package sstore

import (
	"github.com/pkg/errors"
	"os"
	"sync/atomic"
)

type SegmentReader struct {
	f        *os.File
	filename string
	isClose  int32
	release  func()
}

func (s *SegmentReader) Read(p []byte) (n int, err error) {
	return s.f.Read(p)
}

func (s *SegmentReader) Seek(offset int64, whence int) (int64, error) {
	return s.f.Seek(offset, whence)
}

func (s *SegmentReader) Close() error {
	if atomic.CompareAndSwapInt32(&s.isClose, 0, 1) {
		err := s.f.Close()
		s.release()
		return err
	}
	return errors.Errorf("repeated close %s", s.Filename())
}
func (s *SegmentReader) Filename() string {
	return s.filename
}
func (s *SegmentReader) Size() int64 {
	stat, _ := s.f.Stat()
	return stat.Size()
}
