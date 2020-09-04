package client

import (
	"bufio"
	"context"
	"fmt"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"io"
	"sync/atomic"
)

type streamReader struct {
	readBuffSize    int64
	offset          int64
	streamID        int64
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
	stat, err := s.client.GetStreamStat(s.ctx, &proto.GetStreamStatRequest{StreamID: s.streamID})
	if err != nil {
		return 0, err
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
	_ = s.rpcStreamReader.Close()
	s.rpcStreamReader = nil
}

func (s *streamReader) getBufReader() *bufio.Reader {
	if s.reader != nil {
		return s.reader
	}
	if s.rpcStreamReader != nil {
		_ = s.rpcStreamReader.Close()
	}
	s.rpcStreamReader = newRpcStreamReader(s.ctx, s.streamID,
		s.offset, s.readBuffSize, s.client)
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
	streamID    int64
	offset      int64
	bytesToRead int64
	notify      chan interface{}
	ctx         context.Context
	stop        context.CancelFunc
	client      proto.StreamServiceClient
	responses   chan []byte
	minToRead   int64

	readBuffer []byte
}

func newRpcStreamReader(ctx context.Context,
	streamID int64,
	offset int64,
	minToRead int64,
	client proto.StreamServiceClient) *rpcStreamReader {
	reader := &rpcStreamReader{
		streamID:    streamID,
		offset:      offset,
		bytesToRead: 0,
		notify:      make(chan interface{}, 1),
		client:      client,
		minToRead:   minToRead,
		responses:   make(chan []byte, 100),
	}
	reader.ctx, reader.stop = context.WithCancel(ctx)
	go reader.rpcRequestLoop()
	return reader
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
		if size < 0 {
			select {
			case <-r.notify:
				continue
			case <-r.ctx.Done():
				return
			}
		}
		if size < r.minToRead {
			size = r.minToRead
		} else if size > 1024*1024 {
			size = 1024 * 1024
		}
		stream, err := r.client.ReadStream(r.ctx, &proto.ReadStreamRequest{
			StreamId: r.streamID,
			Offset:   r.offset,
			Size:     size,
		})
		if err != nil {
			log.Error(err)
			return
		}
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Warn(err)
				break
			}
			r.offset = response.Offset
			atomic.AddInt64(&r.bytesToRead, -int64(len(response.Data)))
			select {
			case r.responses <- response.Data:
			case <-r.ctx.Done():
				//notify server close stream
				_ = stream.CloseSend()
				log.Warn(err)
				return
			}
		}
	}
}

func (r *rpcStreamReader) Read(p []byte) (n int, err error) {
	var size int
	if atomic.AddInt64(&r.bytesToRead, int64(len(p))) > 0 {
		select {
		case <-r.notify:
		default:
		}
	}
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
