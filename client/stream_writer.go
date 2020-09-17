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
	block_queue "github.com/akzj/streamIO/pkg/block-queue"
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

	requestId    int64
	requestQueue *block_queue.Queue
}

type writeStreamRequest struct {
	requestID int64
	data      []byte
	streamID  int64
	callback  func(err error)
}

type closeNotify struct {
	done func()
}

func newWriteStreamRequest(ctx context.Context, client proto.StreamServiceClient) *streamRequestWriter {
	ctx, cancel := context.WithCancel(ctx)
	return &streamRequestWriter{
		client:       client,
		ctx:          ctx,
		cancel:       cancel,
		requestId:    0,
		requestQueue: block_queue.NewQueue(1024),
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
				request := writer.requestQueue.PopAllWithoutBlock(nil)
				for _, it := range request {
					switch item := it.(type) {
					case writeStreamRequest:
						item.callback(err)
					case closeNotify:
						item.done()
						return
					}
				}
				select {
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
				for _, result := range response.Results {
					locker.Lock()
					request, ok := requestMap[result.RequestId]
					delete(requestMap, result.RequestId)
					locker.Unlock()
					if ok == false {
						log.WithField("requestID",
							result.RequestId).Error("no find request")
					} else {
						if result.Err != "" {
							request.callback(errors.New(result.Err))
						} else {
							request.callback(nil)
						}
					}
				}
			}
		}()
		var entries = make([]*proto.WriteStreamEntry, 0, 64)
		var size int
		appendEntries := func(request writeStreamRequest) {
			request.requestID = writer.getRequestID()
			locker.Lock()
			requestMap[request.requestID] = request
			locker.Unlock()
			entries = append(entries, &proto.WriteStreamEntry{
				Offset:    -1,
				StreamId:  request.streamID,
				Data:      request.data,
				RequestId: request.requestID,
			})
			size += len(request.data) + 24
		}
	writeLoop:
		for {
			items := writer.requestQueue.PopAll(make([]interface{}, 0, 128))
			for _, it := range items {
				switch request := it.(type) {
				case writeStreamRequest:
					appendEntries(request)
					if size >= 1024*1024 || len(entries) >= 256 {
						break
					}
					continue
				case closeNotify:
					request.done()
					return
				}
				if err := stream.Send(&proto.WriteStreamRequest{Entries: entries}); err != nil {
					break writeLoop
				}
				entries = make([]*proto.WriteStreamEntry, 0, 64)
				size = 0
			}
			if len(entries) != 0 {
				if err := stream.Send(&proto.WriteStreamRequest{Entries: entries}); err != nil {
					break writeLoop
				}
				entries = make([]*proto.WriteStreamEntry, 0, 64)
				size = 0
			}
		}
	}
}

func (writer *streamRequestWriter) Close() error {
	var wg sync.WaitGroup
	wg.Add(1)
	writer.requestQueue.Push(closeNotify{
		done: func() {
			wg.Done()
		},
	})
	wg.Wait()
	return nil
}

type streamWriter struct {
	locker     sync.Mutex
	streamInfo *proto.StreamInfoItem
	buffer     bytes.Buffer
	queue      *block_queue.Queue
}

func (s *streamWriter) WriteWithCb(data []byte, callback func(err error)) {
	s.queue.Push(writeStreamRequest{
		data:     data,
		streamID: s.streamInfo.StreamId,
		callback: callback,
	})
}

func (s *streamWriter) StreamID() int64 {
	return s.streamInfo.StreamId
}
