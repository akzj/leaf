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

package mqtt_broker

import (
	"bytes"
	"github.com/akzj/streamIO/client"
	"github.com/akzj/streamIO/proto"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/google/btree"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
)

type subscriber struct {
	streamWriter client.StreamWriter
	sessionID    int64
	qos          int32
	topic        string
	status       *subscriberStatus
	streamInfo   *proto.StreamInfoItem
}

func (s *subscriber) Online() bool {
	return s.status.Status() == proto.ClientStatusChangeEvent_Online
}

func (s *subscriber) Qos() int32 {
	return s.qos
}

func (s *subscriber) ID() int64 {
	return s.sessionID
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) writePacket(packet *packets.PublishPacket, callback func(err error)) {
	if s.Online() == false && packet.Qos == 0 {
		log.WithField("sessionID", s.sessionID).Info("session offline,skip qos0 message")
		callback(nil)
		return
	}
	log.WithField("session", s.sessionID).
		WithField("streamInfo", s.streamInfo).
		WithField("topic", packet.TopicName).
		WithField("Qos", packet.Qos).Debug("write packet")
	var buffer bytes.Buffer
	if err := packet.Write(&buffer); err != nil {
		log.Fatalf("%+v", err)
	}
	s.streamWriter.WriteWithCb(buffer.Bytes(), callback)
}

type subscriberStatus struct {
	sessionID int64
	status    *proto.ClientStatusChangeEvent_Status
}

func (ss subscriberStatus) Status() proto.ClientStatusChangeEvent_Status {
	return proto.ClientStatusChangeEvent_Status(atomic.LoadInt32((*int32)(ss.status)))
}

func (ss *subscriberStatus) Less(item btree.Item) bool {
	return ss.sessionID < item.(*subscriberStatus).sessionID
}
