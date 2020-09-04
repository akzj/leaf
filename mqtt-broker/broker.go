package mqtt_broker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	block_queue "github.com/akzj/block-queue"
	"github.com/akzj/streamIO/client"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"
)

type Broker struct {
	Options
	tlsConfig *tls.Config

	sessionsLocker sync.Mutex
	sessions       map[string]*session

	tree unsafe.Pointer

	EventWatcher *EventReader
	eventQueue   *block_queue.Queue

	client          client.Client
	eventOffset     int64
	treeApplyEvents int64
	isCheckpoint    int32

	eventWriter client.StreamWriter
}

func (broker *Broker) Start() {
	broker.clientListenLoop()
}

func (broker *Broker) getSubscribeTree() *Tree {
	return (*Tree)(atomic.LoadPointer(&broker.tree))
}

func (broker *Broker) setSubscribeTree(tree *Tree) {
	atomic.StorePointer(&broker.tree, unsafe.Pointer(tree))
}

func (broker *Broker) newListener() ([]net.Listener, error) {
	var listeners []net.Listener
	if broker.BindPort != 0 {
		listener, err := net.Listen("tcp",
			net.JoinHostPort(broker.HOST, strconv.Itoa(broker.BindPort)))
		if err != nil {
			log.WithField("broker.BindPort", broker.BindPort).Error(err)
			return nil, err
		}
		listeners = append(listeners, listener)
	}
	if broker.BindTLSPort != 0 {
		listener, err := tls.Listen("tcp",
			net.JoinHostPort(broker.HOST, strconv.Itoa(broker.BindPort)), broker.tlsConfig)
		if err != nil {
			log.WithField("broker.BindTLSPort", broker.BindPort).Error(err)
			return nil, err
		}
		listeners = append(listeners, listener)
	}
	if listeners == nil {
		return nil, errors.New("no listeners")
	}
	return listeners, nil
}

func (broker *Broker) deleteSession(identifier string) error {
	//1 delete client session from meta-server
	info, err := broker.client.DeleteMQTTClientSession(context.Background(), identifier)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	if info == nil {
		return nil
	}

	//2 delete subscribe,and pub `unsubscribe-event` to mqtt-event-queue
	packet := &packets.UnsubscribePacket{}
	for topic := range info.Topics {
		packet.Topics = append(packet.Topics, topic)
	}
	if err := broker.handleUnSubscribePacket(info, packet); err != nil {
		log.Error(err.Error())
		return err
	}
	return nil
}

func (broker *Broker) handleConnection(conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()
	remoteAddr := conn.RemoteAddr()
	var logEntry = log.WithField("remoteAddr", remoteAddr)
	packet, err := packets.ReadPacket(conn)
	if err != nil {
		logEntry.Error(err)
		return
	}
	connectPacket, ok := packet.(*packets.ConnectPacket)
	if ok == false {
		logEntry.Error("first packet is no ConnectPacket error")
		return
	}

	logEntry.WithField("connectPacket", connectPacket).Info("handle connection")

	connackPacket := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)

	//validate connect packet
	if returnCode := connectPacket.Validate(); returnCode != packets.Accepted {
		logEntry.Errorf("Validate failed %s", packets.ConnackReturnCodes[returnCode])
		connackPacket.ReturnCode = returnCode
		connackPacket.Write(conn)
		return
	}
	//check auth
	if status, err := broker.checkConnectAuth(connectPacket.ClientIdentifier,
		connectPacket.Username,
		string(connectPacket.Password)); err != nil {
		connackPacket.ReturnCode = packets.ErrRefusedServerUnavailable
		connackPacket.Write(conn)
		logEntry.Error(err)
		return
	} else if status == false {
		connackPacket.ReturnCode = packets.ErrRefusedNotAuthorised
		connackPacket.Write(conn)
		return
	}
	//process session
	if connectPacket.CleanSession {
		if err := broker.deleteSession(connectPacket.ClientIdentifier); err != nil {
			connackPacket.ReturnCode = packets.ErrRefusedServerUnavailable
			connackPacket.Write(conn)
			logEntry.Error(err)
		}
		//[MQTT-3.1.2-6]。
		defer func() {
			if err := broker.deleteSession(connectPacket.ClientIdentifier); err != nil {
				log.Error(err)
			}
		}()
	}
	if connectPacket.Keepalive == 0 {
		connectPacket.Keepalive = broker.DefaultKeepalive
	}
	sess, err := newSession(broker, connectPacket.Keepalive, conn, broker.client, connectPacket.ClientIdentifier)
	if err != nil {
		log.Error(err.Error())
		connackPacket.ReturnCode = packets.ErrRefusedServerUnavailable
		connackPacket.Write(conn)
		logEntry.Error(err)
		return
	}
	connackPacket.SessionPresent = sess.create
	if err := connackPacket.Write(conn); err != nil {
		return
	}
	go sess.readStreamLoop()
	sess.readConnLoop()
}

func (broker *Broker) serve(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			//todo process error
			continue
		}
		go broker.handleConnection(conn)
	}
}

func (broker *Broker) clientListenLoop() {
	listeners, err := broker.newListener()
	if err != nil {
		log.Panic(err)
	}
	for _, listener := range listeners {
		go broker.serve(listener)
	}
}

func (broker *Broker) checkConnectAuth(clientIdentifier string, username string, password string) (bool, error) {
	return true, nil
}

func (broker *Broker) handleEvent(packet packets.ControlPacket) {
	broker.eventQueue.Push(packet)
}

func (broker *Broker) newSubscriber(event *SubscribeEvent) ([]Subscriber, error) {
	writer, err := broker.client.NewStreamWriter(context.Background(), event.StreamId, event.StreamServerId)
	if err != nil {
		return nil, err
	}
	var subscribers []Subscriber
	for topic, qos := range event.Topic {
		subscribers = append(subscribers, &subscriber{
			streamWriter:   writer,
			streamID:       event.StreamId,
			streamServerID: event.StreamServerId,
			qos:            qos,
			topic:          topic,
		})
	}
	return subscribers, nil
}

func (broker *Broker) handleSubscribeEvent(event *SubscribeEvent) {
	subs, err := broker.newSubscriber(event)
	if err != nil {
		log.Error(err.Error())
		return
	}
	tree := broker.getSubscribeTree().Clone()
	for _, sub := range subs {
		tree.Insert(sub)
	}
	broker.setSubscribeTree(tree)
	broker.eventOffset = event.Offset
}
func (broker *Broker) handleUnSubscribeEvent(event *UnSubscribeEvent) {
	tree := broker.getSubscribeTree().Clone()
	for _, topic := range event.Topic {
		tree.Delete(&subscriber{topic: topic, streamID: event.StreamId})
	}
	broker.setSubscribeTree(tree)
	broker.eventOffset = event.Offset
}

func (broker *Broker) handleRetainMessage(event *RetainMessage) {
	tree := broker.getSubscribeTree().Clone()
	var buffer = bytes.NewReader(event.Data)
	packet, err := packets.ReadPacket(buffer)
	if err != nil {
		log.Error(err.Error())
		return
	}
	tree.UpdateRetainPacket(packet.(*packets.PublishPacket))
	broker.setSubscribeTree(tree)
	broker.eventOffset = event.Offset
}

func (broker *Broker) processEventLoop() {
	for {
		events := broker.eventQueue.PopAll(nil)
		for _, event := range events {
			switch event := event.(type) {
			case *SubscribeEvent:
				broker.handleSubscribeEvent(event)
			case *UnSubscribeEvent:
				broker.handleUnSubscribeEvent(event)
			case *RetainMessage:
				broker.handleRetainMessage(event)
			}
			broker.treeApplyEvents++
		}
		if broker.treeApplyEvents > broker.SubTreeCheckpointEventSize {
			if atomic.CompareAndSwapInt32(&broker.isCheckpoint, 0, 1) == false {
				continue
			}
			clone := broker.getSubscribeTree().Clone()
			go func() {
				defer func() {
					atomic.StoreInt32(&broker.isCheckpoint, 0)
				}()
				if err := broker.checkpoint(clone, broker.eventOffset); err != nil {
					log.Error(err)
					return
				}
				log.Infof("checkpoint success")
			}()
		}
	}
}

func (broker *Broker) getSnapshotFile() (io.WriteCloser, error) {
	return nil, nil
}

func (broker *Broker) checkpoint(clone *Tree, offset int64) error {
	writer, err := broker.getSnapshotFile()
	if err != nil {
		log.Error(err.Error())
	}
	bufio := bufio.NewWriterSize(writer, 1024*1024)
	var data []byte
	clone.Walk(func(path string, subscribers map[int64]Subscriber) bool {
		for _, iter := range subscribers {
			sub := iter.(*subscriber)
			var subEvent = &SubscribeEvent{
				StreamId:       sub.streamID,
				SessionId:      sub.streamID,
				StreamServerId: sub.streamServerID,
				Topic:          map[string]int32{iter.Topic(): iter.Qos()},
				Offset:         offset,
			}
			data, err = proto.Marshal(subEvent)
			if err != nil {
				log.Fatal(err)
			}
			data, err = proto.Marshal(&Event{Data: data, Type: Event_SubscribeEvent})
			if err != nil {
				log.Fatal(err)
			}
			if err = binary.Write(bufio, binary.BigEndian, int32(len(data))); err != nil {
				log.Error(err)
				return false
			}
			if _, err = bufio.Write(data); err != nil {
				log.Error(err)
				return false
			}
		}
		return true
	})

	clone.RangeRetainMessage(func(packet *packets.PublishPacket) bool {
		var buffer bytes.Buffer
		if err = packet.Write(&buffer); err != nil {
			log.Fatal(err)
		}
		data, err = proto.Marshal(&Event{Data: buffer.Bytes(), Type: Event_RetainMessage})
		if err != nil {
			log.Fatal(err.Error())
		}
		if err = binary.Write(bufio, binary.BigEndian, int32(len(data))); err != nil {
			log.Error(err.Error())
			return false
		}
		if _, err = bufio.Write(data); err != nil {
			log.Error(err)
			return false
		}
		return true
	})
	if err = bufio.Flush(); err != nil {
		log.Error(err)
		return err
	}
	if err = writer.Close(); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (broker *Broker) handleUnSubscribePacket(MqttSessionItem *store.MQTTSessionItem, packet *packets.UnsubscribePacket) error {
	event := UnSubscribeEvent{
		StreamId:       MqttSessionItem.StreamId,
		SessionId:      MqttSessionItem.SessionId,
		StreamServerId: MqttSessionItem.StreamId,
		Topic:          packet.Topics,
	}
	data, err := proto.Marshal(&event)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Wait()
	broker.eventWriter.WriteWithCb(data, func(e error) {
		wg.Done()
		err = e
	})
	wg.Wait()
	return err
}

func (broker *Broker) handleSubscribePacket(MqttSessionItem *store.MQTTSessionItem, packet *packets.SubscribePacket) error {
	event := SubscribeEvent{
		StreamId:       MqttSessionItem.StreamId,
		SessionId:      MqttSessionItem.SessionId,
		StreamServerId: MqttSessionItem.StreamId,
		Topic:          map[string]int32{},
	}
	for index, topic := range packet.Topics {
		qos := packet.Qoss[index]
		event.Topic[topic] = int32(qos)
	}
	data, err := proto.Marshal(&event)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Wait()
	broker.eventWriter.WriteWithCb(data, func(e error) {
		wg.Done()
		err = e
	})
	wg.Wait()
	return err
}
