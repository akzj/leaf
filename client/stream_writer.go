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
	"bytes"
	"context"
	"github.com/akzj/streamIO/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type streamRequestWriter struct {
	client proto.StreamServiceClient
	ctx    context.Context
	cancel context.CancelFunc

	requestId int64
	requests  chan writeStreamRequest
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
		client:    client,
		ctx:       ctx,
		cancel:    cancel,
		requestId: 0,
		requests:  make(chan writeStreamRequest),
	}
}

func (writer *streamRequestWriter) getRequestID() int64 {
	writer.requestId++
	return writer.requestId
}

func (writer *streamRequestWriter) writeLoop() {
	for {
		stream, err := writer.client.WriteStream(writer.ctx)
		if err != nil {
			log.Errorf(err.Error())
		Loop:
			for {
				select {
				case request := <-writer.requests:
					request.callback(err)
				case <-time.After(time.Second):
					break Loop
				case <-writer.ctx.Done():
					return
				}
			}
			continue
		}
		requestMap := make(map[int64]writeStreamRequest)
		var locker sync.Mutex
		go func() {
			for {
				response, err := stream.Recv()
				if err != nil {
					log.Error(err)
					locker.Lock()
					for _, it := range requestMap {
						delete(requestMap, it.requestID)
						it.callback(err)
					}
					locker.Unlock()
					return
				}
				locker.Lock()
				request, ok := requestMap[response.RequestId]
				delete(requestMap, response.RequestId)
				locker.Unlock()
				if ok == false {
					log.WithField("requestID",
						response.RequestId).Error("no find request")
				} else {
					if response.Err != "" {
						request.callback(errors.New(response.Err))
					} else {
						request.callback(nil)
					}
				}
			}
		}()
	writeLoop:
		for {
			select {
			case <-writer.ctx.Done():
				log.Error(writer.ctx.Err())
			case request := <-writer.requests:
				request.requestID = writer.getRequestID()
				locker.Lock()
				requestMap[request.requestID] = request
				locker.Unlock()
				if err := stream.Send(&proto.WriteStreamRequest{
					StreamId:  request.streamID,
					Offset:    -1,
					Data:      request.data,
					RequestId: request.requestID,
				}); err != nil {
					break writeLoop
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
	streamInfo          *proto.StreamInfoItem
	buffer              bytes.Buffer
	writeStreamRequests chan<- writeStreamRequest
}

func (s *streamWriter) WriteWithCb(data []byte, callback func(err error)) {
	s.writeStreamRequests <- writeStreamRequest{
		data:     data,
		streamID: s.streamInfo.StreamId,
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

func (s *streamWriter) StreamID() int64 {
	return s.streamInfo.StreamId
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
