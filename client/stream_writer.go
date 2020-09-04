package client

import (
	"bytes"
	"context"
	block_queue "github.com/akzj/block-queue"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type streamRequestWriter struct {
	queue  *block_queue.Queue
	client proto.StreamServiceClient
	ctx    context.Context
	cancel context.CancelFunc

	requestId int64
}

type writeStreamRequest struct {
	data     []byte
	streamID int64
	close    bool
	callback func(err error)
}

func newWriteStreamRequest(ctx context.Context, client proto.StreamServiceClient) *streamRequestWriter {
	ctx, cancel := context.WithCancel(ctx)
	return &streamRequestWriter{
		queue:  block_queue.NewQueue(1024),
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (writer *streamRequestWriter) writeLoop() {
	var close bool
	for close == false {
		stream, err := writer.client.WriteStream(writer.ctx)
		if err != nil {
			log.Errorf(err.Error())
			select {
			case <-writer.ctx.Done():
				log.Error(writer.ctx.Err())
			case <-time.After(time.Second):
			}
			continue
		}
		for close == false {
			items := writer.queue.PopAll(nil)
			for _, item := range items {
				request := item.(writeStreamRequest)
				if request.close {
					close = true
				}
				writer.requestId++
				if err := stream.Send(&proto.WriteStreamRequest{
					StreamId:  request.streamID,
					Offset:    -1,
					Data:      request.data,
					RequestId: writer.requestId,
				}); err != nil {
					log.Error(err)
					request.callback(err)
				} else {
					request.callback(nil)
				}
			}
		}
		if err := stream.CloseSend(); err != nil {
			log.Error(err)
		}
	}
}

func (writer *streamRequestWriter) Close() error {
	writer.queue.Push(writeStreamRequest{
		close: true,
	})
	return nil
}

type streamWriter struct {
	locker   sync.Mutex
	streamID int64
	buffer   bytes.Buffer
	queue    *block_queue.Queue
}

func (s *streamWriter) WriteWithCb(data []byte, callback func(err error)) {
	s.queue.Push(writeStreamRequest{
		data:     s.buffer.Bytes(),
		streamID: s.streamID,
		close:    false,
		callback: callback,
	})
}

func (s *streamWriter) Write(data []byte) (n int, err error) {
	s.locker.Lock()
	defer s.locker.Unlock()
	if s.buffer.Len()+len(data) >= minWriteSize {
		if err := s.flushWithoutLock(); err != nil {
			return 0, err
		}
	}
	return s.buffer.Write(data)
}

func (s *streamWriter) Close() error {
	return s.Flush()
}

func (s *streamWriter) Flush() error {
	s.locker.Lock()
	defer s.locker.Unlock()
	return s.flushWithoutLock()
}

func (s *streamWriter) flushWithoutLock() error {
	var err error
	if s.buffer.Len() > 0 {
		var wg sync.WaitGroup
		wg.Add(1)
		s.queue.Push(writeStreamRequest{
			data:     s.buffer.Bytes(),
			streamID: s.streamID,
			close:    false,
			callback: func(e error) {
				err = e
				wg.Done()
			},
		})
		s.buffer.Reset()
		wg.Wait()
	}
	return err
}
