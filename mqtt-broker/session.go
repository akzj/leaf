package mqtt_broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/akzj/streamIO/client"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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

	client      client.Client
	ctx         context.Context
	cancel      context.CancelFunc
	closeOnce   sync.Once
	willMessage *packets.PublishPacket
	log         *log.Entry

	streamPacketReaders [2]*streamPacketReader
}

func PPrintln(obj interface{}) {
	data, _ := json.Marshal(obj)
	var buffer bytes.Buffer
	json.Indent(&buffer, data, "  ", " ")
	fmt.Println(string(buffer.String()))
}

func newSession(broker *Broker,
	keepalive uint16,
	conn net.Conn,
	client client.Client,
	clientIdentifier string) (*session, error) {

	ctx, cancel := context.WithCancel(context.Background())
	info, create, err := client.GetOrCreateMQTTSession(ctx, clientIdentifier)
	if err != nil {
		return nil, err
	}
	PPrintln(info)
	_, end, err := client.GetStreamStat(ctx, info.Qos0StreamInfo)
	if err != nil {
		return nil, err
	}
	if end > 0 {
		if err := client.SetStreamReadOffset(ctx, info.SessionId, end, info.Qos0StreamInfo); err != nil {
			return nil, err
		}
	}
	if err := broker.handleClientStatusChange(
		info.SessionId, ClientStatusChangeEvent_Online); err != nil {
		return nil, err
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
		closeOnce:       sync.Once{},
		willMessage:     nil,
		log: log.WithField("remoteAddr", conn.RemoteAddr().
			String()).WithField("clientIdentifier", clientIdentifier),
		streamPacketReaders: [2]*streamPacketReader{},
	}, nil
}

func (sess *session) startStreamPacketReader(qos int32) {
	if sess.streamPacketReaders[qos] != nil {
		return
	}
	packetReader, err := newStreamPacketReader(sess.ctx, sess,
		sess.MQTTSessionInfo.Qos0StreamInfo, qos, sess.client)
	if err != nil {
		_ = sess.Close()
		log.Errorf("%+v", err)
		return
	}
	sess.streamPacketReaders[qos] = packetReader
	go func() {
		if err := packetReader.readPacketLoop(); err != nil {
			log.Errorf(err.Error())
		}
	}()
}

func (sess *session) run() {
	for _, qos := range sess.MQTTSessionInfo.Topics {
		sess.startStreamPacketReader(qos)
	}
	go sess.readConnLoop()
}

func (sess *session) readConnLoop() {
	logEntry := sess.log.WithField("remoteAddr", sess.conn.RemoteAddr().String())
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
			sess.log.Errorf("%+v", err)
			_ = sess.Close()
			return
		}
	}
}

func (sess *session) handlePacket(controlPacket packets.ControlPacket) error {
	switch packet := controlPacket.(type) {
	case *packets.PubackPacket:
		return sess.handlePubAckPacket(packet)
	case *packets.PublishPacket:
		return sess.handlePublishPacket(packet)
	case *packets.PubrelPacket:
		return nil
	case *packets.SubscribePacket:
		return sess.handleSubscribePacket(packet)
	case *packets.UnsubscribePacket:
		return sess.handleUnsubscribePacket(packet)
	case *packets.DisconnectPacket:
		return sess.handleDisconnectPacket(packet)
	case *packets.PingreqPacket:
		return sess.handlePingReqPacket(packet)
	}
	return nil
}

func minQos(q1, q2 int32) int32 {
	if q1 < q2 {
		return q1
	}
	return q2
}

func (sess *session) sendPacket(packet packets.ControlPacket) error {
	sess.connWLocker.Lock()
	defer sess.connWLocker.Unlock()
	if err := packet.Write(sess.conn); err != nil {
		return errors.WithStack(err)
	}
	return nil
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
					sess.log.Warn(err)
					atomic.StorePointer(&errPointer, unsafe.Pointer(&err))
				}
				wg.Done()
			})
		}
	}
	wg.Wait()
	if eObj := atomic.LoadPointer(&errPointer); eObj != nil {
		return errors.WithStack(*(*error)(eObj))
	}
	return nil
}

func (sess *session) setWillMessage(packet *packets.PublishPacket) {
	sess.willMessage = packet
}

func (sess *session) handlePublishPacketQos0(packet *packets.PublishPacket) error {
	return sess.sendPacket2Subscribers(packet)
}

func (sess *session) handlePublishPacketQos1(packet *packets.PublishPacket) error {
	if err := sess.sendPacket2Subscribers(packet); err != nil {
		return err
	} else {
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := sess.sendPacket(puback); err != nil {
			sess.log.Errorf("%+v", err)
			return err
		}
	}
	return nil
}

func (sess *session) handlePublishPacketQos2(packet *packets.PublishPacket) error {
	if err := sess.sendPacket2Subscribers(packet); err != nil {
		return err
	} else {
		pubRec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
		pubRec.MessageID = packet.MessageID
		if err := sess.sendPacket(pubRec); err != nil {
			sess.log.Errorf("%+v", err)
			return err
		}
		pubComp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
		pubComp.MessageID = packet.MessageID
		if err := sess.sendPacket(pubComp); err != nil {
			sess.log.Errorf("%+v", err)
			return err
		}
	}
	return nil
}

func (sess *session) handlePublishPacket(packet *packets.PublishPacket) error {
	log.WithField("packet", packet).Info("handlePublishPacket")
	if packet.Retain {
		if err := sess.broker.handleRetainPacket(packet); err != nil {
			sess.log.Errorf("%+v", err)
			return err
		}
	}
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

func (sess *session) handleOutPublishPacket(packet *packets.PublishPacket) error {
	log.Infof("pub message %s topic %s ", string(packet.Payload), packet.TopicName)
	if err := sess.sendPacket(packet); err != nil {
		return err
	}
	return nil
}

func (sess *session) handleSubscribePacket(packet *packets.SubscribePacket) error {

	sess.log.WithField("packet", packet).Info("handleSubscribePacket")

	subAck := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	subAck.MessageID = packet.MessageID

	if err := sess.broker.handleSubscribePacket(sess.MQTTSessionInfo, packet); err != nil {
		sess.log.Errorf("%+v", err)
		return err
	} else {
		var Topic = make(map[string]int32)
		for index, topic := range packet.Topics {
			qos := packet.Qoss[index]
			Topic[topic] = int32(qos)
		}
		if err := sess.client.UpdateMQTTClientSession(sess.ctx,
			sess.MQTTSessionInfo.ClientIdentifier, nil, Topic); err != nil {
			sess.log.Errorf("%+v", err)
			return err
		}
	}
	for index := range packet.Topics {
		qos := packet.Qoss[index]
		if qos == 2 {
			qos = 1
		}
		sess.startStreamPacketReader(int32(qos))
		subAck.ReturnCodes = append(subAck.ReturnCodes, qos)
	}
	if err := sess.sendPacket(subAck); err != nil {
		sess.log.Errorf("%+v", err)
		return err
	}
	return nil
}

func (sess *session) handleDisconnectPacket(_ *packets.DisconnectPacket) error {
	sess.willMessage = nil
	return fmt.Errorf("session disconnect")
}

func (sess *session) handlePingReqPacket(_ *packets.PingreqPacket) error {
	return sess.sendPacket(packets.NewControlPacket(packets.Pingresp))
}

func (sess *session) handleUnsubscribePacket(packet *packets.UnsubscribePacket) error {
	for _, topic := range packet.Topics {
		delete(sess.MQTTSessionInfo.Topics, topic)
	}
	for qos, reader := range sess.streamPacketReaders {
		if reader == nil {
			continue
		}
		var find bool
		for _, val := range sess.MQTTSessionInfo.Topics {
			if int(val) == qos {
				find = true
			}
		}
		if find == false {
			log.Infof("close qos %d streamPacketReader", qos)
			reader.Close()
		}
		sess.streamPacketReaders[qos] = nil
	}
	if err := sess.broker.handleUnSubscribePacket(sess.MQTTSessionInfo, packet); err != nil {
		return err
	}
	if err := sess.client.UpdateMQTTClientSession(sess.ctx,
		sess.MQTTSessionInfo.ClientIdentifier, packet.Topics, nil); err != nil {
		return err
	}

	ack := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	ack.MessageID = packet.MessageID
	return sess.sendPacket(ack)
}

func (sess *session) handlePubAckPacket(packet *packets.PubackPacket) error {
	if reader := sess.streamPacketReaders[1]; reader != nil {
		reader.handleAck(packet.MessageID)
	}
	return nil
}

func (sess *session) Close() error {
	var err error
	sess.closeOnce.Do(func() {
		if err := sess.broker.handleClientStatusChange(
			sess.MQTTSessionInfo.SessionId, ClientStatusChangeEvent_Offline); err != nil {
			sess.log.Errorf("%+v", err)
		}
		if sess.willMessage != nil {
			if err = sess.handlePublishPacket(sess.willMessage); err != nil {
				sess.log.Errorf("%+v", err)
			}
		}
		err = sess.conn.Close()
		sess.cancel()
	})
	return err
}
