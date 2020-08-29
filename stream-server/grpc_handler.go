package stream_server

import (
	"github.com/akzj/sstore"
	"github.com/akzj/streamIO/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"strings"
)

func (server *StreamServer) ReadStream(request *proto.ReadStreamRequest, stream proto.StreamService_ReadStreamServer) error {
	for {
		err := server.store.ReadRequest(stream.Context(), request, func(offset int64, data []byte) error {
			if err := stream.Send(&proto.ReadStreamResponse{}); err != nil {
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
