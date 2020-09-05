package client

import (
	"bytes"
	"context"
	"github.com/akzj/streamIO/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"sync"
	"time"
)

type streamRequestWriter struct {
	client proto.StreamServiceClient
	ctx    context.Context
	cancel context.CancelFunc

	requestId          int64
	requests           chan writeStreamRequest
	locker             sync.Mutex
	writeStreamRequest map[int64]writeStreamRequest
}

type writeStreamRequest struct {
	requestID int64
	data      []byte
	streamID  int64
	callback  func(err error)
}

func newWriteStreamRequest(ctx context.Context, client proto.StreamServiceClient) *streamRequestWriter {
	ctx, cancel := context.WithCancel(ctx)
	return &streamRequestWriter{
		client:             client,
		ctx:                ctx,
		cancel:             cancel,
		requestId:          0,
		writeStreamRequest: map[int64]writeStreamRequest{},
	}
}
func (writer *streamRequestWriter) readResponse(stream proto.StreamService_WriteStreamClient) {
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Error(err)
			_ = stream.CloseSend()
			return
		}
		writer.locker.Lock()
		request, ok := writer.writeStreamRequest[response.RequestId]
		writer.locker.Unlock()
		if ok == false {
			log.WithField("requestID", response.RequestId).Error("no find request")
		} else {
			if response.Err != "" {
				request.callback(errors.New(response.Err))
			} else {
				request.callback(nil)
			}
		}
	}
}

func (writer *streamRequestWriter) getRequestID() int64 {
	writer.requestId++
	return writer.requestId
}

func (writer *streamRequestWriter) addRequest(request writeStreamRequest) {
	writer.locker.Lock()
	writer.writeStreamRequest[request.requestID] = request
	writer.locker.Unlock()
}

func (writer *streamRequestWriter) requestCallback(err error) {
	writer.locker.Lock()
	for _, it := range writer.writeStreamRequest {
		it.callback(err)
	}
	writer.locker.Unlock()
}

func (writer *streamRequestWriter) writeLoop() {
	for {
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
		writer.readResponse(stream)
		for {
			select {
			case <-writer.ctx.Done():
			case request := <-writer.requests:
				request.requestID = writer.getRequestID()
				writer.addRequest(request)
				if err := stream.Send(&proto.WriteStreamRequest{
					StreamId:  request.streamID,
					Offset:    -1,
					Data:      request.data,
					RequestId: request.requestID,
				}); err != nil {
					log.Error(err)
					writer.requestCallback(err)
					break
				}
			}
		}
	}
}

func (writer *streamRequestWriter) Close() error {
	close(writer.requests)
	return nil
}

type streamWriter struct {
	locker              sync.Mutex
	streamID            int64
	buffer              bytes.Buffer
	writeStreamRequests chan<- writeStreamRequest
}

func (s *streamWriter) WriteWithCb(data []byte, callback func(err error)) {
	s.writeStreamRequests <- writeStreamRequest{
		data:     s.buffer.Bytes(),
		streamID: s.streamID,
		callback: callback,
	}
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
		s.WriteWithCb(s.buffer.Bytes(), func(e error) {
			err = e
			wg.Done()
		})
		s.buffer.Reset()
		wg.Wait()
	}
	return err
}
