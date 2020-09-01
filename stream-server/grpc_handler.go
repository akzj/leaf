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
	for {
		err := server.store.ReadRequest(stream.Context(), request, func(offset int64, data []byte) error {
			if err := stream.Send(&proto.ReadStreamResponse{
				Offset: offset,
				Data:   data,
			}); err != nil {
				//todo log error
				return err
			}
			return nil
		})
		if err == nil {
			return nil
		}
		//todo log error
		if strings.Contains(err.Error(), sstore.ErrNoFindStream.Error()) {
			return status.Errorf(codes.NotFound, err.Error())
		}
		if strings.Contains(err.Error(), sstore.ErrOffset.Error()) {
			return status.Errorf(codes.FailedPrecondition, err.Error())
		}
		return nil
	}
}

func (server *StreamServer) WriteStream(stream proto.StreamService_WriteStreamServer) error {
	var requestError error
	for requestError == nil {
		request, err := stream.Recv()
		if err == io.EOF {
			//todo log error
			return nil
		}
		if err != nil {
			//todo log error
			return err
		}
		server.store.WriteRequest(request, func(offset int64, err error) {
			if err != nil {
				if err == sstore.ErrOffset {
					//todo log error
					requestError = status.Error(codes.FailedPrecondition, err.Error())
					return
				}
				err = status.Error(codes.ResourceExhausted, err.Error())
				//todo log error
				return
			}
			err = stream.Send(&proto.WriteStreamResponse{
				Offset:    offset,
				RequestId: request.RequestId,
			})
			if err != nil {
				//todo log error
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
