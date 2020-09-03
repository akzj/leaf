package mqtt_broker

import (
	"bytes"
	block_queue "github.com/akzj/block-queue"
	"github.com/akzj/streamIO/client"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

type subscriber struct {
	streamWriter client.StreamWriter
	streamID     int64
	topic        string
	queue        *block_queue.Queue
}

func (s *subscriber) ID() int64 {
	return s.streamID
}

func (s *subscriber) Topic() string {
	return s.topic
}

const (
	MQTTPacket int32 = 1
)

func (s *subscriber) writePacket(packet *packets.PublishPacket, callback func(err error)) {
	var data bytes.Buffer
	packet.Write(&data)
	s.streamWriter.WriteWithCb(data.Bytes(), callback)
}
