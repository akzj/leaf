package mqtt_broker

import (
	"bytes"
	"github.com/akzj/streamIO/client"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/google/btree"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
)

type subscriber struct {
	streamWriter   client.StreamWriter
	streamID       int64
	streamServerID int64
	clientIdentify string
	qos            int32
	topic          string
	status         *subscriberStatus
}

func (s *subscriber) Online() bool {
	return s.status.Status() == ClientStatusChangeEvent_Online
}

func (s *subscriber) Qos() int32 {
	return s.qos
}

func (s *subscriber) ID() int64 {
	return s.streamID
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) writePacket(packet *packets.PublishPacket, callback func(err error)) {
	if s.Online() == false && packet.Qos == 0 {
		callback(nil)
		return
	}
	var buffer bytes.Buffer
	if err := packet.Write(&buffer); err != nil {
		log.Fatal("%+v", err)
	}
	s.streamWriter.WriteWithCb(buffer.Bytes(), callback)
}

type subscriberStatus struct {
	clientIdentifier string
	status           *ClientStatusChangeEvent_Status
}

func (ss subscriberStatus) Status() ClientStatusChangeEvent_Status {
	return ClientStatusChangeEvent_Status(atomic.LoadInt32((*int32)(ss.status)))
}

func (ss *subscriberStatus) Less(item btree.Item) bool {
	return ss.clientIdentifier < item.(*subscriberStatus).clientIdentifier
}
