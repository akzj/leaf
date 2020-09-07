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
			var heartbeat proto.StreamServerHeartbeatItem
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
