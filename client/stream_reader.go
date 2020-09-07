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
package client

import (
	"bufio"
	"context"
	"fmt"
	"github.com/akzj/streamIO/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync/atomic"
	"time"
)

type streamReader struct {
	readBuffSize    int64
	offset          int64
	rpcStreamReader *rpcStreamReader
	reader          *bufio.Reader
	client          proto.StreamServiceClient
	ctx             context.Context
	session         *session
}

func (s *streamReader) Offset() int64 {
	return s.offset
}

func (s *streamReader) Seek(offset int64, whence int) (int64, error) {
	request := &proto.GetStreamStatRequest{StreamID: s.session.streamInfo.StreamId}
	stat, err := s.client.GetStreamStat(s.ctx, request)
	if err != nil {
		if status.Convert(err).Code() == codes.NotFound {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += s.offset
	case io.SeekEnd:
		offset += stat.End
	}
	if offset > stat.End || offset < stat.Begin {
		return 0, fmt.Errorf("offset out of stream range [%d,%d]", stat.Begin, stat.End)
	}
	if s.offset != offset {
		s.reset()
	}
	s.offset = offset
	return s.offset, nil
}

func (s *streamReader) reset() {
	if s.rpcStreamReader != nil {
		_ = s.rpcStreamReader.Close()
		s.rpcStreamReader = nil
	}
}

func (s *streamReader) getBufReader() *bufio.Reader {
	if s.reader != nil {
		return s.reader
	}
	if s.rpcStreamReader != nil {
		_ = s.rpcStreamReader.Close()
	}
	s.rpcStreamReader = newRpcStreamReader(s.ctx,
		s.session.streamInfo, s.offset, s.readBuffSize, s.client)
	s.reader = bufio.NewReader(s.rpcStreamReader)
	return s.reader
}

func (s *streamReader) Read(p []byte) (n int, err error) {
	n, err = s.getBufReader().Read(p)
	s.offset += int64(n)
	return n, err
}

func (s *streamReader) Close() error {
	s.reset()
	return nil
}

type rpcStreamReader struct {
	streamInfo  *proto.StreamInfoItem
	offset      int64
	bytesToRead int64
	ctx         context.Context
	stop        context.CancelFunc
	client      proto.StreamServiceClient
	responses   chan []byte
	minToRead   int64
	maxToRead   int64
	readBuffer  []byte
}

func newRpcStreamReader(ctx context.Context,
	streamInfo *proto.StreamInfoItem,
	offset int64,
	minToRead int64,
	client proto.StreamServiceClient) *rpcStreamReader {
	reader := &rpcStreamReader{
		streamInfo:  streamInfo,
		offset:      offset,
		bytesToRead: 0,
		ctx:         nil,
		stop:        nil,
		client:      client,
		responses:   make(chan []byte, 4),
		minToRead:   minToRead,
		maxToRead:   1024 * 1024,
		readBuffer:  nil,
	}
	reader.ctx, reader.stop = context.WithCancel(ctx)
	go reader.rpcRequestLoop()
	return reader
}

func sleepWithCtx(ctx context.Context, duration time.Duration) error {
	if duration == 0 {
		duration = time.Second
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(duration):
		return nil
	}
}

func (r *rpcStreamReader) rpcRequestLoop() {
	for {
		select {
		case <-r.ctx.Done():
			log.Error(r.ctx.Err())
			return
		default:
		}
		size := atomic.LoadInt64(&r.bytesToRead)
		if size < r.minToRead {
			size = r.minToRead
		} else if size > r.maxToRead {
			size = r.maxToRead
		}
		stream, err := r.client.ReadStream(r.ctx, &proto.ReadStreamRequest{
			StreamId: r.streamInfo.StreamId,
			Offset:   r.offset,
			Size:     size,
			Watch:    true,
		})
		if err != nil {
			log.Error(err)
			if err := sleepWithCtx(r.ctx, time.Second); err != nil {
				log.Error(err.Error())
				return
			}
			continue
		}
	readLoop:
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				switch status.Convert(err).Code() {
				case codes.Canceled:
					return
				case codes.NotFound:
					time.Sleep(time.Second)
					break readLoop
				default:
					log.Warn(err)
					time.Sleep(time.Second)
					break readLoop
				}
			}
			r.offset = response.Offset
			atomic.AddInt64(&r.bytesToRead, -int64(len(response.Data)))
			select {
			case r.responses <- response.Data:
			case <-r.ctx.Done():
				log.Warn(err)
				return
			}
		}
	}
}

func (r *rpcStreamReader) Read(p []byte) (n int, err error) {
	atomic.StoreInt64(&r.bytesToRead, int64(len(p)))
	var size int
	for len(p) > 0 {
		if r.readBuffer != nil {
			n := copy(p, r.readBuffer)
			size += n
			p = p[n:]
			r.readBuffer = r.readBuffer[n:]
			if len(r.readBuffer) == 0 {
				r.readBuffer = nil
			}
		} else {
			if size > 0 {
				return size, nil
			}
			select {
			case r.readBuffer = <-r.responses:
				continue
			case <-r.ctx.Done():
				return size, r.ctx.Err()
			}
		}
	}
	return size, nil
}

func (r *rpcStreamReader) Close() error {
	r.stop()
	return nil
}
