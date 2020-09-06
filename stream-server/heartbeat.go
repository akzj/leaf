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
	var stream proto.MetaService_StreamServerHeartbeatClient
	for {
		select {
		case <-time.Tick(server.Options.HeartbeatInterval):
		case <-server.ctx.Done():
			log.Warn(server.ctx.Err())
			return
		}
		if stream == nil {
			var err error
			if stream, err = client.StreamServerHeartbeat(server.ctx); err != nil {
				log.Errorf("%+v", err)
				continue
			}
		}
		now := time.Now()
		var heartbeat store.StreamServerHeartbeatItem
		heartbeat.Base = server.ServerInfoBase
		heartbeat.Timestamp = &timestamp.Timestamp{
			Seconds: now.Unix(),
			Nanos:   0,
		}
		if err := stream.Send(&heartbeat); err != nil {
			log.Warn(err)
			stream = nil
			continue
		}
		if _, err := stream.Recv(); err != nil {
			log.Error(err)
			stream = nil
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
