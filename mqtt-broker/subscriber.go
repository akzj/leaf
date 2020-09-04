package mqtt_broker

import (
	"bytes"
	"github.com/akzj/streamIO/client"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

type subscriber struct {
	streamWriter   client.StreamWriter
	streamID       int64
	streamServerID int64
	qos            int32
	topic          string
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
	var data bytes.Buffer
	packet.Write(&data)
	s.streamWriter.WriteWithCb(data.Bytes(), callback)
}
