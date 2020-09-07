package mqtt_broker

import (
	"bytes"
	"github.com/akzj/streamIO/client"
	"github.com/akzj/streamIO/meta-server/store"
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
	streamInfo   *store.StreamInfoItem
}

func (s *subscriber) Online() bool {
	return s.status.Status() == ClientStatusChangeEvent_Online
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
		WithField("Qos", packet.Qos).Info("write packet")
	var buffer bytes.Buffer
	if err := packet.Write(&buffer); err != nil {
		log.Fatalf("%+v", err)
	}
	s.streamWriter.WriteWithCb(buffer.Bytes(), callback)
}

type subscriberStatus struct {
	sessionID int64
	status    *ClientStatusChangeEvent_Status
}

func (ss subscriberStatus) Status() ClientStatusChangeEvent_Status {
	return ClientStatusChangeEvent_Status(atomic.LoadInt32((*int32)(ss.status)))
}

func (ss *subscriberStatus) Less(item btree.Item) bool {
	return ss.sessionID < item.(*subscriberStatus).sessionID
}
