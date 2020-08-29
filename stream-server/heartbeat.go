package stream_server

import (
	"context"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/grpc"
	"time"
)

func (server StreamServer) GetMetaServiceClient(ctx context.Context) proto.MetaServiceClient {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, server.Options.MetaServerAddr)
	if err != nil {
		//todo log error
		return nil
	}
	client := proto.NewMetaServiceClient(conn)
	return client
}

func (server *StreamServer) sendHeartbeat(client proto.MetaServiceClient) {
	stream, err := client.StreamServerHeartbeat(server.ctx)
	if err != nil {
		//todo log error
		return
	}
	tick := time.NewTicker(server.Options.HeartbeatInterval)
	defer func() {
		tick.Stop()
		if err := stream.CloseSend(); err != nil {
			//todo log error
		}
	}()
	for {
		select {
		case <-tick.C:
		case <-server.ctx.Done():
			//todo log error
			return
		}
		now := time.Now()
		var heartbeatItem store.StreamServerHeartbeatItem
		heartbeatItem.ServerInfoBase = server.ServerInfoBase
		heartbeatItem.Timestamp = &timestamp.Timestamp{
			Seconds: now.Unix(),
			Nanos:   0,
		}
		if err := stream.Send(&heartbeatItem); err != nil {
			//todo log error
		}
	}
}

func (server *StreamServer) heartbeatLoop() {
	for {
		client := server.GetMetaServiceClient(server.ctx)
		if client == nil {
			select {
			case <-time.After(time.Second):
			case <-server.ctx.Done():
				//todo log error
				return
			}
			continue
		}
		server.sendHeartbeat(client)
	}
}
