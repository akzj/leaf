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
	"time"
	"unsafe"
)

type session struct {
	MQTTSessionInfo *store.MQTTSessionItem
	create          bool //create
	keepalive       uint16
	broker          *Broker

	connWLocker sync.Mutex
	conn        net.Conn
	limiter     chan interface{}

	client client.Client
	ctx    context.Context
	cancel context.CancelFunc

	ackOffset int64
	offset    int64

	ackMapLocker sync.Mutex
	ackMap       map[uint16]int64

	streamReader  client.StreamReader
	streamSession client.StreamSession

	closeOnce sync.Once
}

//todo to
func newSession(broker *Broker,
	keepalive uint16,
	conn net.Conn,
	client client.Client,
	clientIdentifier string) (*session, error) {

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := client.GetOrCreateStream(ctx, clientIdentifier); err != nil {
		log.Error(err.Error())
		return nil, err
	}

	info, create, err := client.GetOrCreateMQTTSession(ctx, clientIdentifier)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	//
	streamSession, err := client.NewStreamSession(ctx, 0, clientIdentifier)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	offset, err := streamSession.GetReadOffset()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	reader, err := streamSession.NewReader()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if offset != 0 {
		if _, err := reader.Seek(offset, io.SeekStart); err != nil {
			log.Error(err)
			return nil, err
		}
	}
	return &session{
		MQTTSessionInfo: info,
		create:          create,
		keepalive:       keepalive,
		broker:          broker,
		connWLocker:     sync.Mutex{},
		conn:            conn,
		limiter:         make(chan interface{}, 10),
		client:          client,
		ctx:             ctx,
		cancel:          cancel,
		ackOffset:       offset,
		offset:          offset,
		ackMapLocker:    sync.Mutex{},
		ackMap:          map[uint16]int64{},
		streamReader:    reader,
		streamSession:   streamSession,
		closeOnce:       sync.Once{},
	}, nil
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
	case *packets.PubrelPacket:
	case *packets.SubscribePacket:
		sess.handleSubscribePacket(p)
	}
	return nil
}

func minQos(q1, q2 int32) int32 {
	if q1 < q2 {
		return q1
	}
	return q2
}

func (sess *session) sendPacket2Subscribers(packet *packets.PublishPacket) error {
	detail := packet.Details()
	var errPointer unsafe.Pointer
	var wg sync.WaitGroup
	tree := sess.broker.getSubscribeTree()
	for _, subMaps := range tree.Match(packet.TopicName) {
		for _, sub := range subMaps {
			wg.Add(1)
			packet.Qos = byte(minQos(int32(detail.Qos), sub.Qos()))
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
	return sess.sendPacket2Subscribers(packet)
}

func (sess *session) handlePublishPacketQos1(packet *packets.PublishPacket) error {
	if err := sess.sendPacket2Subscribers(packet); err != nil {
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
	if err := sess.sendPacket2Subscribers(packet); err != nil {
		//todo handle error
	} else {
		puback := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
		puback.MessageID = packet.MessageID
		if err := sess.sendPacket(puback); err != nil {
			log.Error(err.Error())
			//todo process error
		}
		packet := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
		packet.MessageID = packet.MessageID
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

func (sess *session) getSessionName() string {
	return fmt.Sprintf("MQTT|%s", sess.MQTTSessionInfo.ClientIdentifier)
}

func (sess *session) handleOutPublishPacket(packet *packets.PublishPacket) {
	switch packet.Details().Qos {
	case 0:
		if err := sess.sendPacket(packet); err != nil {
			log.Error(err.Error())
			_ = sess.Close()
		}
		atomic.StoreInt64(&sess.ackOffset, sess.offset)
	case 1:
		if err := sess.sendPacket(packet); err != nil {
			log.Error(err.Error())
			_ = sess.Close()
		}
		sess.ackMapLocker.Lock()
		sess.ackMap[packet.MessageID] = sess.offset
		sess.ackMapLocker.Unlock()
	}
}

func (sess *session) handleSubscribePacket(packet *packets.SubscribePacket) {
	subackPacket := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	subackPacket.MessageID = packet.MessageID
	retCode := byte(0x01)
	if err := sess.broker.handleSubscribePacket(sess.MQTTSessionInfo, packet); err != nil {
		log.Error(err)
		retCode = 0x80
	} else {
		var Topic = make(map[string]int32)
		for index, topic := range packet.Topics {
			qos := packet.Qoss[index]
			Topic[topic] = int32(qos)
		}
		if err := sess.client.UpdateMQTTClientSession(sess.ctx,
			sess.MQTTSessionInfo.ClientIdentifier,
			nil, Topic); err != nil {
			log.Error(err.Error())
		}
	}
	for range packet.Topics {
		subackPacket.ReturnCodes = append(subackPacket.ReturnCodes, retCode)
	}
	if err := sess.sendPacket(subackPacket); err != nil {
		log.Error(err.Error())
		_ = sess.Close()
	}
}

func (sess *session) readConnLoop() {
	logEntry := log.WithField("remoteAddr", sess.conn.RemoteAddr().String())
	logEntry = logEntry.WithField("ClientIdentifier", sess.MQTTSessionInfo.ClientIdentifier)
	defer func() {
		_ = sess.Close()
	}()
	for {
		keepalive := time.Duration(sess.keepalive) * time.Second
		if err := sess.conn.SetReadDeadline(time.Now().Add(keepalive + keepalive/2)); err != nil {
			logEntry.Errorf("SetReadDeadline failed %s", err.Error())
			return
		}
		packet, err := packets.ReadPacket(sess.conn)
		if err != nil {
			logEntry.Errorf("packets.ReadPacket failed %s", err.Error())
			return
		}
		if err := sess.handlePacket(packet); err != nil {
			packets.NewControlPacket(packets.Disconnect).Write(sess.conn)
			return
		}
	}
}

func (sess *session) readStreamLoop() {
	sess.ackOffset = sess.offset
	lastAckOffset := sess.ackOffset
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			}
			if lastAckOffset == atomic.LoadInt64(&sess.ackOffset) {
				continue
			}
			lastAckOffset = atomic.LoadInt64(&sess.ackOffset)
			if err := sess.streamSession.SetReadOffset(lastAckOffset); err != nil {
				log.Error(err)
			}
		}
	}()
	for {
		controlPacket, err := packets.ReadPacket(sess.streamReader)
		if err != nil {
			log.Error(err)
			if err := sleep(sess.ctx); err != nil {
				log.Error(err.Error()) //ctx is cancel
				return
			}
			continue
		}
		atomic.StoreInt64(&sess.offset, sess.streamReader.Offset())
		switch packet := controlPacket.(type) {
		case *packets.PublishPacket:
			sess.handleOutPublishPacket(packet)
		}
	}
}

func (sess *session) Close() error {
	var err error
	sess.closeOnce.Do(func() {
		err = sess.conn.Close()
		sess.cancel()
	})
	return err
}
