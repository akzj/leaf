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

package stream_server

import (
	"context"
	"fmt"
	"github.com/akzj/streamIO/pkg/sstore"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/akzj/streamIO/proto"
	"github.com/akzj/streamIO/stream-server/ssyncer"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"strings"
	"sync/atomic"
	"time"
)

func (server *StreamServer) ReadStream(request *proto.ReadStreamRequest, stream proto.StreamService_ReadStreamServer) error {
	log.WithField("request", request).Info("handle request")
	var size int64
	err := server.store.ReadRequest(stream.Context(), request, func(offset int64, data []byte) error {
		if err := stream.Send(&proto.ReadStreamResponse{
			Offset: offset,
			Data:   data,
		}); err != nil {
			log.Error(err)
			return err
		}
		size += int64(len(data))
		log.WithField("offset", offset).
			WithField("size", size).
			WithField("streamID", request.StreamId).
			WithField("data len", len(data)).Debug("response")
		return nil
	})
	if err == nil {
		log.WithField("request", request).Info("end")
		return nil
	}
	log.WithField("request", request).Warn(err)
	if strings.Contains(err.Error(), sstore.ErrNoFindStream.Error()) {
		return status.Errorf(codes.NotFound, err.Error())
	}
	if strings.Contains(err.Error(), sstore.ErrOffset.Error()) {
		return status.Errorf(codes.FailedPrecondition, err.Error())
	}
	return nil
}

func (server *StreamServer) WriteStream(stream proto.StreamService_WriteStreamServer) error {
	var streamResults = make(chan *proto.WriteStreamResult, 64)
	var ctx = stream.Context()
	var results = make([]*proto.WriteStreamResult, 0, 64)
	var pendingCh = make(chan struct{}, 1)
	go func() {
		for {
			select {
			case result := <-streamResults:
				results = append(results, result)
			case <-ctx.Done():
				log.Error(ctx.Err())
				return
			}
			for {
				select {
				case result := <-streamResults:
					results = append(results, result)
				case <-ctx.Done():
					log.Error(ctx.Err())
					return
				case pendingCh <- struct{}{}:
					goto responseResult
				}
			}

		responseResult:
			if err := stream.Send(&proto.WriteStreamResponse{Results: results}); err != nil {
				log.Error(err)
				return
			}
			<-pendingCh
			results = make([]*proto.WriteStreamResult, 0, 64)
		}
	}()
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			log.Warn(err)
			return nil
		}
		if err != nil {
			log.Warn(err)
			return err
		}

		for _, entry := range request.Entries {
			entry := entry
			server.store.WriteRequest(entry, func(offset int64, writerErr error) {
				atomic.AddInt64(&server.count, 1)
				result := &proto.WriteStreamResult{
					Offset:    offset,
					StreamId:  entry.StreamId,
					RequestId: entry.RequestId,
				}
				if writerErr != nil {
					result.Err = writerErr.Error()
				}
				select {
				case streamResults <- result:
				case <-ctx.Done():
					log.Error(ctx.Err())
				}
			})
		}
	}
}

func (server *StreamServer) printMsgCount() {
	var lastCount int64
	for {
		time.Sleep(time.Second)
		msgCount := atomic.LoadInt64(&server.count)
		fmt.Println(msgCount - lastCount)
		lastCount = msgCount
	}
}

func (server *StreamServer) GetStreamStat(_ context.Context, request *proto.GetStreamStatRequest) (*proto.GetStreamStatResponse, error) {
	stat, err := server.store.GetStreamStat(request.StreamID)
	if err != nil {
		log.Warningf("GetStreamStat(%d) failed %s", request.StreamID, err.Error())
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &proto.GetStreamStatResponse{End: stat.End,
		Begin:    stat.Begin,
		StreamID: stat.StreamID}, nil
}

func (server *StreamServer) StartSyncFrom(_ context.Context, request *proto.SyncFromRequest) (*empty.Empty, error) {
	server.syncClientLocker.Lock()
	defer server.syncClientLocker.Unlock()
	if server.syncClient != nil {
		server.syncClient.Stop()
	}
	server.syncClient = ssyncer.NewClient(server.store.GetSStore())
	go func() {
		if err := server.syncClient.Start(server.ctx, server.ServerID, request.Addr); err != nil {
			log.Error(err)
			server.syncStreamFailed(request.Addr, err)
		}
	}()
	return &empty.Empty{}, nil
}

func (server *StreamServer) GetStreamStoreVersion(ctx context.Context, request *proto.GetStreamStoreVersionRequest) (*pb.Version, error) {
	return server.store.GetSStore().Version(), nil
}
