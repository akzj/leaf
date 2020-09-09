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
	"context"
	"github.com/eclipse/paho.mqtt.golang/packets"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

//The total number of bytes received since the broker started.
const loadBytesReceived = `$SYS/broker/load/bytes/received`

//The total number of bytes sent since the broker started.
const loadBytesSent = `$SYS/broker/load/bytes/sent`

//The number of currently connected clients
const clientsConnected = `$SYS/broker/clients/connected`

//The total number of persistent clients (with clean session disabled) that are registered at the broker but are currently disconnected.
const clientsDisconnected = `$SYS/broker/clients/disconnected`

//The maximum number of active clients that have been connected to the broker.
//This is only calculated when the $SYS topic tree is updated, so short lived client connections may not be counted.
const clientsMaximum = `$SYS/broker/clients/maximum`

//The total number of connected and disconnected clients with a persistent session currently connected and registered on the broker.
const clientsTotal = `$SYS/broker/clients/total`

//The total number of messages of any type received since the broker started.
const messagesReceived = `$SYS/broker/messages/received`

//The total number of messages of any type sent since the broker started.
const messagesSent = `$SYS/broker/messages/sent`

//The total number of publish messages that have been dropped due to inflight/queuing limits.
const messagesPublishDropped = `$SYS/broker/messages/publish/dropped`

//The total number of PUBLISH messages received since the broker started.
const messagesPublishReceived = `$SYS/broker/messages/publish/received`

//The total number of PUBLISH messages sent since the broker started.
const messagesPublishSent = `$SYS/broker/messages/publish/sent`

//The total number of retained messages active on the broker.
const messagesRetainedCount = `$SYS/broker/messages/retained/count`

//The total number of subscriptions active on the broker.
const subscriptionsCount = `$SYS/broker/subscriptions/count`

// The amount of time in seconds the broker has been online.
const uptime = `$SYS/broker/uptime`

//The version of the broker. Static.
const version = `$SYS/broker/version`

type SYS struct {
	broker    *Broker
	messageID uint16
}

func newSysPub(broker *Broker) *SYS {
	return &SYS{
		broker: broker,
	}
}

func (sys *SYS) pubLoop(ctx context.Context, interval time.Duration) {
	if interval == 0 {
		interval = time.Minute * 5
	}
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
		log.Info("publish $SYS topic")
		sys.pubSys()
	}
}

func (sys SYS) toBytes(val int64) []byte {
	return []byte(strconv.FormatInt(val, 10))
}

func (sys *SYS) createLoadBytesReceived() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = loadBytesReceived
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getLoadBytesReceived())
	return packet
}

func (sys *SYS) createLoadBytesSent() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = loadBytesSent
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getLoadBytesSent())
	return packet
}

func (sys *SYS) createClientsConnected() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = clientsConnected
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getClientCount())
	return packet
}

func (sys *SYS) createClientsDisconnected() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = clientsDisconnected
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getClientsDisconnected())
	return packet
}

func (sys *SYS) createClientsMaximum() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = clientsMaximum
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getClientsMaximum())
	return packet
}

func (sys *SYS) createClientsTotal() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = clientsTotal
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getClientsTotal())
	return packet
}

func (sys *SYS) createMessagesReceived() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = messagesReceived
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getMessagesReceived())
	return packet
}

func (sys *SYS) createMessagesSent() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = messagesSent
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getMessagesSent())
	return packet
}

func (sys *SYS) createMessagesPublishDropped() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = messagesPublishDropped
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(0)
	return packet
}

func (sys *SYS) createMessagesPublishReceived() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = messagesPublishReceived
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getMessagesPublishReceived())
	return packet
}

func (sys *SYS) createMessagesPublishSent() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = messagesPublishSent
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getMessagesPublishSent())
	return packet
}

func (sys *SYS) createMessagesRetainedCount() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = messagesRetainedCount
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.getMessagesRetainedCount())
	return packet
}

func (sys *SYS) createSubscriptionsCount() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = subscriptionsCount
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(sys.broker.SubscriptionsCount())
	return packet
}

var uptimeTS = time.Now()

func (sys *SYS) createUptime() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = uptime
	packet.MessageID = sys.messageID
	packet.Payload = sys.toBytes(int64(time.Now().Sub(uptimeTS).Seconds()))
	return packet
}

func (sys *SYS) createVersion() *packets.PublishPacket {
	sys.messageID++
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = version
	packet.MessageID = sys.messageID
	packet.Payload = []byte(Version)
	return packet
}

func (sys *SYS) pubSys() {
	for _, packet := range []*packets.PublishPacket{sys.createLoadBytesReceived(),
		sys.createLoadBytesSent(),
		sys.createClientsConnected(),
		sys.createClientsDisconnected(),
		sys.createClientsMaximum(),
		sys.createClientsTotal(),
		sys.createMessagesReceived(),
		sys.createMessagesSent(),
		sys.createMessagesPublishDropped(),
		sys.createMessagesPublishReceived(),
		sys.createMessagesPublishSent(),
		sys.createMessagesRetainedCount(),
		sys.createSubscriptionsCount(),
		sys.createUptime(),
		sys.createVersion()} {
		if err := sys.broker.handlePublishPacket(packet); err != nil {
			log.Error(err)
		}
	}
}
