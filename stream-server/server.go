package stream_server

import (
	"github.com/akzj/sstore"
	"github.com/akzj/streamIO/proto"
	"github.com/akzj/streamIO/stream-server/store"
	"io"
)

type StreamServer struct {
	store *store.Store
}

func (server *StreamServer) ReadStream(request *proto.ReadStreamRequest, stream proto.StreamService_ReadStreamServer) error {
	for {
		err := server.store.ReadRequest(stream.Context(), request, func(offset int64, data []byte) error {
			if err := stream.Send(&proto.ReadStreamResponse{}); err != nil {
				//todo log error
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
}

func (server *StreamServer) WriteStream(stream proto.StreamService_WriteStreamServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		server.store.WriteRequest(request, func(offset int64, err error) {
			if err != nil {
				if err == sstore.ErrOffset {
					err = stream.Send(&proto.WriteStreamResponse{
						Status:    proto.WriteStreamResponse_OffsetError,
						RequestId: request.RequestId,
					})
					if err != nil {
						//todo log error
					}
					return
				}
				err = stream.Send(&proto.WriteStreamResponse{
					Status:    proto.WriteStreamResponse_OffsetError,
					RequestId: request.RequestId,
				})
				if err != nil {
					//todo log error
				}
				return
			}
			err = stream.Send(&proto.WriteStreamResponse{
				Status:    proto.WriteStreamResponse_ok,
				Offset:    offset,
				RequestId: request.RequestId,
			})
			if err != nil {
				return
			}
		})
	}
}
