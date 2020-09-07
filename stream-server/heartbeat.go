package stream_server

import (
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	log "github.com/sirupsen/logrus"
	"time"
)


func (server *StreamServer) heartbeatLoop() {
	for {
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
				if stream, err = server.metaServiceClient.StreamServerHeartbeat(server.ctx); err != nil {
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
}
