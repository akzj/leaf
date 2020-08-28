package stream_server

import (
	context "context"
	"github.com/akzj/sstore"
	"github.com/akzj/streamIO/proto"
)

type StreamServer struct {
	store *sstore.SStore
}

func (server *StreamServer) WriteStream(writeStreamServer proto.StreamService_WriteStreamServer) error {
	panic("implement me")
}

func (server *StreamServer) ReadStream(readStreamServer proto.StreamService_ReadStreamServer) error {
	panic("implement me")
}

func (server *StreamServer) StopReadStream(ctx context.Context, readStreamRequest *proto.ReadStreamRequest) (*proto.StopReadStreamResponse, error) {
	panic("implement me")
}
