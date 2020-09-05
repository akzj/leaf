package stream_server

import (
	"context"
	"github.com/akzj/sstore"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"strings"
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
		log.WithField("offset", offset).WithField("size", size).WithField("data len", len(data)).Debug("response")
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
	var requestError error
	for requestError == nil {
		request, err := stream.Recv()
		if err == io.EOF {
			log.Warn(err)
			return nil
		}
		if err != nil {
			log.Warn(err)
			return err
		}
		server.store.WriteRequest(request, func(offset int64, writerErr error) {
			response := &proto.WriteStreamResponse{
				StreamId:  request.StreamId,
				Offset:    offset,
				RequestId: request.RequestId,
			}
			if writerErr != nil {
				response.Err = writerErr.Error()
			}
			if err = stream.Send(response); err != nil {
				log.Warn(err)
				requestError = err
			}
		})
	}
	return requestError
}

func (server *StreamServer) GetStreamStat(ctx context.Context, request *proto.GetStreamStatRequest) (*proto.GetStreamStatResponse, error) {
	stat, err := server.store.GetStreamStat(request.StreamID)
	if err != nil {
		log.Warningf("GetStreamStat(%d) failed %s", request.StreamID, err.Error())
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &proto.GetStreamStatResponse{End: stat.End,
		Begin:    stat.Begin,
		StreamID: stat.StreamID}, nil
}
