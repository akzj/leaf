package stream_server

import (
	"context"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"time"
)

func (server *StreamServer) GetMetaServiceClient(ctx context.Context) proto.MetaServiceClient {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, server.Options.MetaServerAddr, grpc.WithInsecure())
	if err != nil {
		log.Warn(err)
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
			log.Warn(err)
		}
	}()
	for {
		select {
		case <-tick.C:
		case <-server.ctx.Done():
			log.Warn(err)
			return
		}
		now := time.Now()
		var heartbeatItem store.StreamServerHeartbeatItem
		heartbeatItem.Base = server.ServerInfoBase
		heartbeatItem.Timestamp = &timestamp.Timestamp{
			Seconds: now.Unix(),
			Nanos:   0,
		}
		if err := stream.Send(&heartbeatItem); err != nil {
			log.Warn(err)
			return
		}
		if _, err := stream.Recv(); err != nil {
			log.Error(err)
			return
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
				log.Warn(server.ctx.Err())
				return
			}
			continue
		}
		server.sendHeartbeat(client)
	}
}
