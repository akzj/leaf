package mqtt_broker

import (
	"context"
	"fmt"
	"github.com/akzj/streamIO/client"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"unsafe"
)

type session struct {
	base      store.MQTTSession
	create    bool //create
	keepalive uint16
	broker    *Broker

	connWLocker sync.Mutex
	conn        net.Conn
	limiter     chan interface{}

	client client.Client
	ctx    context.Context
}

func (sess *session) handlePacket(packet packets.ControlPacket) error {
	switch p := packet.(type) {
	case *packets.PublishPacket:
		sess.limiter <- struct{}{}
		go func() {
			defer func() {
				<-sess.limiter
			}()
			_ = sess.handlePublishPacket(p)
		}()
	}
	return nil
}

func (sess *session) transformPacket2Subscriber(packet *packets.PublishPacket) error {
	var errPointer unsafe.Pointer
	var wg sync.WaitGroup
	tree := sess.broker.getSubscribeTree()
	for _, subMaps := range tree.Match(packet.TopicName) {
		for _, sub := range subMaps {
			wg.Add(1)
			sub.writePacket(packet, func(err error) {
				if err != nil {
					log.Warn(err)
				}
				atomic.StorePointer(&errPointer, unsafe.Pointer(&err))
				wg.Done()
			})
		}
	}
	wg.Wait()
	if eObj := atomic.LoadPointer(&errPointer); eObj != nil {
		return *(*error)(eObj)
	}
	return nil
}

func (sess *session) handlePublishPacketQos0(packet *packets.PublishPacket) error {
	return sess.transformPacket2Subscriber(packet)
}

func (sess *session) handlePublishPacketQos1(packet *packets.PublishPacket) error {
	if err := sess.transformPacket2Subscriber(packet); err != nil {
		//todo handle error
	} else {
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := sess.sendPacket(puback); err != nil {
			log.Error(err)
		}
	}
	return nil
}

func (sess *session) sendPacket(packet packets.ControlPacket) error {
	sess.connWLocker.Lock()
	defer sess.connWLocker.Unlock()
	return packet.Write(sess.conn)
}

func (sess *session) handlePublishPacketQos2(packet *packets.PublishPacket) error {
	if err := sess.transformPacket2Subscriber(packet); err != nil {
		//todo handle error
	} else {
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := sess.sendPacket(puback); err != nil {
			log.Error(err)
		}
	}
	return nil
}

func (sess *session) handlePublishPacket(packet *packets.PublishPacket) error {
	switch packet.Qos {
	case 0:
		return sess.handlePublishPacketQos0(packet)
	case 1:
		return sess.handlePublishPacketQos1(packet)
	case 2:
		return sess.handlePublishPacketQos2(packet)
	default:
		return errors.Errorf("QoS %d error", packet.Qos)
	}
}

func (sess *session) Close() error {
	return sess.conn.Close()
}

func (sess session) getSessionName() string {
	return fmt.Sprintf("MQTT|%s", sess.base.ClientIdentifier)
}

func (sess *session) init() error {
	_, err := sess.client.GetOrCreateStream(sess.ctx, sess.getSessionName())
	if err != nil {
		log.Error(err.Error())
		return err
	}
	//todo create mqtt session from meta-server
	return nil
}

func (sess *session) readStreamLoop() {
	streamSession, err := sess.client.NewStreamSession(sess.ctx, sess.base.SessionId, sess.getSessionName())
	if err != nil {
		log.Error(err.Error())
		//todo handle error
	}
	for {
		offset, err := streamSession.GetReadOffset()
		if err != nil {
			log.Error(err)
			//todo handle error
		}
		reader, err := streamSession.NewReader()
		if err != nil {
			log.Error(err)
			//todo handle error
		}
		if offset != 0 {
			if _, err := reader.Seek(offset, io.SeekStart); err != nil {
				log.Error(err)
				//todo handle error
			}
		}
		packet, err := packets.ReadPacket(reader)
		if err != nil {
			log.Error(err)
			//todo handle error
		}
		//todo process packet
		sess.sendPacket(packet)
	}
}
