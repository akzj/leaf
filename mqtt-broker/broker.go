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
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/akzj/block-queue"
	"github.com/akzj/streamIO/client"
	"github.com/akzj/streamIO/pkg/listenutils"
	"github.com/akzj/streamIO/proto"
	"github.com/eclipse/paho.mqtt.golang/packets"
	pproto "github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Broker struct {
	Options
	tlsConfig   *tls.Config
	WSTLSConfig *tls.Config

	sessionsLocker sync.Mutex
	sessions       map[string]*session
	maxClientCount int64

	topicTree unsafe.Pointer
	metaTree  *btree.BTree

	eventReader *EventReader
	eventQueue  *block_queue.Queue

	client       client.Client
	eventOffset  int64
	treeChanges  int64
	isCheckpoint int32

	eventWriter client.StreamWriter

	snapshot *Snapshot

	ctx    context.Context
	cancel context.CancelFunc

	listener []net.Listener

	offsetCommitter *offsetCommitter

	messagesReceived int64
	messagesSent     int64

	loadBytesReceived int64
	loadBytesSent     int64

	messagesPublishReceived int64
	messagesPublishSent     int64

	sys *SYS
}

func New(options Options) *Broker {
	if options.LogFile != "" {
		_ = os.MkdirAll(filepath.Dir(options.LogFile), 0777)
		file, err := os.OpenFile(options.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		log.SetOutput(file)
	}
	log.SetFormatter(&log.TextFormatter{DisableQuote: true})
	log.SetReportCaller(true)
	ctx, cancel := context.WithCancel(context.Background())
	metaServerClient, err := client.NewMetaServiceClient(ctx, options.MetaServerAddr)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	cli := client.NewClient(metaServerClient)
	streamInfoItem, err := cli.GetOrCreateStreamInfoItem(ctx, MQTTEventStream)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	sess, err := cli.NewStreamSession(ctx, options.BrokerId, streamInfoItem)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	eventWriter, err := sess.NewWriter()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	eventQueue := block_queue.NewQueue(128)
	eventWatcher, err := newEventReader(options.BrokerId, cli, func(message EventWithOffset) {
		eventQueue.Push(message)
	})
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	broker := &Broker{
		Options:         options,
		tlsConfig:       nil,
		sessionsLocker:  sync.Mutex{},
		sessions:        map[string]*session{},
		topicTree:       unsafe.Pointer(NewTopicTree()),
		metaTree:        btree.New(10),
		eventReader:     eventWatcher,
		eventQueue:      eventQueue,
		client:          cli,
		eventOffset:     0,
		treeChanges:     0,
		isCheckpoint:    0,
		eventWriter:     eventWriter,
		snapshot:        NewSnapshot(options.SnapshotPath),
		ctx:             ctx,
		cancel:          cancel,
		listener:        nil,
		offsetCommitter: newOffsetCommitter(),
	}

	return broker
}

func (broker *Broker) Start() error {
	if err := broker.snapshot.reloadSnapshot(broker); err != nil {
		return err
	}
	go broker.eventReader.readEventLoop()
	if err := broker.clientTCPListenLoop(); err != nil {
		return err
	}
	if err := broker.webSocketListenLoop(); err != nil {
		return err
	}
	go broker.offsetCommitter.commitLoop(broker.ctx, broker.ReadOffsetCommitInterval)
	broker.sys = newSysPub(broker)
	go broker.sys.pubLoop(broker.ctx, broker.SysInterval)
	broker.processEventLoop()
	return nil
}

func (broker *Broker) serve(listener net.Listener) {
	var tempDelay time.Duration
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Errorf("http: Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
		}
		go broker.handleConnection(conn)
	}
}

func (broker *Broker) webSocketListenLoop() error {
	if broker.WSPort != 0 {
		l, err := listenutils.Listen(broker.HOST, broker.WSPort, nil)
		if err != nil {
			return err
		}
		go func() {
			if err := http.Serve(l, broker); err != nil {
				log.Fatalf("%+v", err)
			}
		}()
	}
	if broker.WSSPort != 0 {
		l, err := listenutils.Listen(broker.HOST, broker.WSSPort, broker.WSTLSConfig)
		if err != nil {
			return err
		}
		go func() {
			if err := http.Serve(l, broker); err != nil {
				log.Fatalf("%+v", err)
			}
		}()
	}
	return nil
}

func (broker *Broker) clientTCPListenLoop() error {
	if broker.BindTLSPort != 0 {
		l, err := listenutils.Listen(broker.HOST, broker.BindTLSPort, broker.tlsConfig)
		if err != nil {
			return err
		}
		go broker.serve(l)
	}
	if broker.BindPort != 0 {
		l, err := listenutils.Listen(broker.HOST, broker.BindPort, nil)
		if err != nil {
			return err
		}
		go broker.serve(l)
	}
	return nil
}

func (broker *Broker) getSubscribeTree() *TopicTree {
	return (*TopicTree)(atomic.LoadPointer(&broker.topicTree))
}

func (broker *Broker) setSubscribeTree(tree *TopicTree) {
	atomic.StorePointer(&broker.topicTree, unsafe.Pointer(tree))
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
		_ = connackPacket.Write(conn)
		return
	}
	//check auth
	if status, err := broker.checkConnectAuth(connectPacket.ClientIdentifier,
		connectPacket.Username,
		string(connectPacket.Password)); err != nil {
		connackPacket.ReturnCode = packets.ErrRefusedServerUnavailable
		_ = connackPacket.Write(conn)
		logEntry.Error(err)
		return
	} else if status == false {
		connackPacket.ReturnCode = packets.ErrRefusedNotAuthorised
		_ = connackPacket.Write(conn)
		return
	}
	//process session
	if connectPacket.CleanSession {
		if err := broker.deleteSession(connectPacket.ClientIdentifier); err != nil {
			connackPacket.ReturnCode = packets.ErrRefusedServerUnavailable
			_ = connackPacket.Write(conn)
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
	} else if connectPacket.Keepalive < broker.MinKeepalive {
		connectPacket.Keepalive = broker.MinKeepalive
	}
	sess, err := newSession(broker, connectPacket.Keepalive, conn, broker.client, connectPacket.ClientIdentifier)
	if err != nil {
		log.Error(err.Error())
		connackPacket.ReturnCode = packets.ErrRefusedServerUnavailable
		_ = connackPacket.Write(conn)
		logEntry.Error(err)
		return
	}
	connackPacket.SessionPresent = sess.create
	connackPacket.ReturnCode = packets.Accepted
	if err := connackPacket.Write(conn); err != nil {
		return
	}
	if connectPacket.WillFlag {
		willMessage := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		willMessage.Payload = connectPacket.WillMessage
		willMessage.TopicName = connectPacket.WillTopic
		sess.setWillMessage(willMessage)
	}

	broker.sessionsLocker.Lock()
	broker.sessions[sess.MQTTSessionInfo.ClientIdentifier] = sess
	count := len(broker.sessions)
	broker.sessionsLocker.Unlock()

	if int64(count) > atomic.LoadInt64(&broker.maxClientCount) {
		atomic.StoreInt64(&broker.maxClientCount, int64(count))
	}
	defer func() {
		broker.sessionsLocker.Lock()
		delete(broker.sessions, sess.MQTTSessionInfo.ClientIdentifier)
		broker.sessionsLocker.Unlock()
	}()
	sess.readConnLoop()
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

func (broker *Broker) getClientCount() int64 {
	broker.sessionsLocker.Lock()
	count := len(broker.sessions)
	broker.sessionsLocker.Unlock()
	return int64(count)
}

func (broker *Broker) getClientsMaximum() int64 {
	return atomic.LoadInt64(&broker.maxClientCount)
}

func (broker *Broker) getMessagesRetainedCount() int64 {
	var count int64
	broker.getSubscribeTree().RangeRetainMessage(func(packet *packets.PublishPacket) bool {
		count++
		return true
	})
	return count
}

func (broker *Broker) getClientsDisconnected() int64 {
	min := &subscriberStatus{sessionID: 0}
	max := &subscriberStatus{sessionID: math.MaxInt64}
	var count int64
	broker.metaTree.DescendRange(min, max, func(item btree.Item) bool {
		status := item.(*subscriberStatus)
		if status.brokerId == broker.BrokerId &&
			status.Status() == proto.ClientStatusChangeEvent_Offline {
			count++
		}
		return true
	})
	return count
}

func (broker *Broker) getMessagesPublishSent() int64 {
	return atomic.LoadInt64(&broker.messagesPublishSent)
}

func (broker *Broker) getClientsTotal() int64 {
	min := &subscriberStatus{sessionID: 0}
	max := &subscriberStatus{sessionID: math.MaxInt64}
	var count int64
	broker.metaTree.DescendRange(min, max, func(item btree.Item) bool {
		status := item.(*subscriberStatus)
		if status.brokerId == broker.BrokerId {
			count++
		}
		return true
	})
	return count
}

func (broker *Broker) getMessagesReceived() int64 {
	return atomic.LoadInt64(&broker.messagesReceived)
}

func (broker *Broker) checkConnectAuth(clientIdentifier string, username string, password string) (bool, error) {
	return true, nil
}

func (broker *Broker) handleEvent(packet packets.ControlPacket) {
	broker.eventQueue.Push(packet)
}

func (broker *Broker) newSubscriber(event *proto.SubscribeEvent) ([]Subscriber, error) {
	item := broker.metaTree.Get(&subscriberStatus{sessionID: event.SessionId})
	if item == nil {
		return nil, fmt.Errorf("no find session status")
	}
	var subscribers []Subscriber
	for topic, qos := range event.Topic {
		streamInfo := event.Qos0StreamInfo
		if qos == 1 {
			streamInfo = event.Qos1StreamInfo
		}
		session, err := broker.client.NewStreamSession(context.Background(), event.SessionId, streamInfo)
		if err != nil {
			return nil, err
		}
		writer, err := session.NewWriter()
		if err != nil {
			return nil, err
		}
		subscribers = append(subscribers, &subscriber{
			streamWriter: writer,
			sessionID:    event.SessionId,
			qos:          qos,
			topic:        topic,
			status:       item.(*subscriberStatus),
			streamInfo:   streamInfo,
		})
	}
	return subscribers, nil
}

func (broker *Broker) handleSubscribeEvent(event *proto.SubscribeEvent) {
	log.WithField("event", event).Info("handleSubscribeEvent")
	tree := broker.getSubscribeTree().Clone()
	broker.insertSubscriber2Tree(tree, event)
	broker.setSubscribeTree(tree)
}

func (broker *Broker) getLoadBytesSent() int64 {
	return atomic.LoadInt64(&broker.loadBytesSent)
}

func (broker *Broker) getLoadBytesReceived() int64 {
	return atomic.LoadInt64(&broker.loadBytesReceived)
}

func (broker *Broker) getMessagesSent() int64 {
	return atomic.LoadInt64(&broker.messagesSent)
}

func (broker *Broker) SubscriptionsCount() int64 {
	var count int64
	broker.getSubscribeTree().Walk(func(path string, subscribers map[int64]Subscriber) bool {
		for _, sub := range subscribers {
			if sub.BrokerID() == broker.BrokerId {
				count++
			}
		}
		return true
	})
	return count
}

func (broker *Broker) getMessagesPublishReceived() int64 {
	return atomic.LoadInt64(&broker.messagesPublishReceived)
}

func (broker *Broker) insertSubscriber2Tree(tree *TopicTree, event *proto.SubscribeEvent) {
	subs, err := broker.newSubscriber(event)
	if err != nil {
		log.Errorf("%+v\n", err)
		return
	}
	for _, sub := range subs {
		tree.Insert(sub)
	}
	//publish retain packets to new subscriber
	for topic, qos := range event.Topic {
		broker.getSubscribeTree().MatchRetainMessage(topic, func(packet *packets.PublishPacket) {
			for _, it := range subs {
				if it.Qos() == minQos(int32(packet.Qos), qos) {
					it.writePacket(packet, func(err error) {
						log.Error(err)
					})
					break
				}
			}
		})
	}
}

func (broker *Broker) handleUnSubscribeEvent(event *proto.UnSubscribeEvent) {
	log.WithField("event", event).Info("handleUnSubscribeEvent")
	tree := broker.getSubscribeTree().Clone()
	for _, topic := range event.Topic {
		tree.Delete(&subscriber{topic: topic, sessionID: event.SessionId})
	}
	broker.setSubscribeTree(tree)
}

func (broker *Broker) handleRetainMessageEvent(event *proto.RetainMessageEvent) {
	log.WithField("event", event).Info("handleRetainMessageEvent")
	tree := broker.getSubscribeTree().Clone()
	_ = broker.insertRetainMessage2Tree(tree, event)
	broker.setSubscribeTree(tree)
}

func (broker *Broker) insertRetainMessage2Tree(tree *TopicTree, event *proto.RetainMessageEvent) error {
	var buffer = bytes.NewReader(event.Data)
	packet, err := packets.ReadPacket(buffer)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	tree.UpdateRetainPacket(packet.(*packets.PublishPacket))
	return nil
}

func (broker *Broker) processEventLoop() {
	for {
		items := broker.eventQueue.PopAll(nil)
		for _, item := range items {
			event := item.(EventWithOffset)
			switch event := event.event.(type) {
			case *proto.SubscribeEvent:
				broker.handleSubscribeEvent(event)
			case *proto.UnSubscribeEvent:
				broker.handleUnSubscribeEvent(event)
			case *proto.RetainMessageEvent:
				broker.handleRetainMessageEvent(event)
			case *proto.ClientStatusChangeEvent:
				broker.handleClientStatusChangeEvent(event)
			default:
				log.Fatalf("unknown event %+v\n", event)
			}
			broker.eventOffset = event.offset
			atomic.AddInt64(&broker.treeChanges, 1)
		}
		if changes := atomic.LoadInt64(&broker.treeChanges);
			changes > broker.CheckpointEventSize {
			if atomic.CompareAndSwapInt32(&broker.isCheckpoint, 0, 1) == false {
				continue
			}
			clone := broker.getSubscribeTree().Clone()
			go func() {
				defer func() {
					atomic.StoreInt32(&broker.isCheckpoint, 0)
				}()
				if err := broker.checkpoint(clone, broker.eventOffset); err != nil {
					log.WithError(err).Errorf("checkpoint failed")
					return
				}
				atomic.AddInt64(&broker.treeChanges, -changes)
				log.Infof("checkpoint success")
			}()
		}
	}
}

func (broker *Broker) handleUnSubscribePacket(sessionItem *proto.MQTTSessionItem,
	packet *packets.UnsubscribePacket) error {
	event := proto.UnSubscribeEvent{
		SessionId: sessionItem.SessionId,
		Topic:     packet.Topics,
	}
	return broker.sendEvent(&event)
}

func (broker *Broker) sendEvent(message pproto.Message) error {
	event := proto.Event{}
	switch typ := message.(type) {
	case *proto.SubscribeEvent:
		event.Type = proto.Event_SubscribeEvent
	case *proto.UnSubscribeEvent:
		event.Type = proto.Event_UnSubscribeEvent
	case *proto.RetainMessageEvent:
		event.Type = proto.Event_RetainMessageEvent
	case *proto.ClientStatusChangeEvent:
		event.Type = proto.Event_ClientStatusChangeEvent
	default:
		panic("unknown message type" + typ.String())
	}
	data, err := pproto.Marshal(message)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	event.Data = data
	data, err = pproto.Marshal(&event)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	var buffer bytes.Buffer
	_ = binary.Write(&buffer, binary.BigEndian, int32(len(data)))
	buffer.Write(data)
	var wg sync.WaitGroup
	wg.Wait()
	wg.Add(1)
	broker.eventWriter.WriteWithCb(buffer.Bytes(), func(e error) {
		wg.Done()
		err = e
	})
	wg.Wait()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (broker *Broker) handlePublishPacket(packet *packets.PublishPacket) error {
	detail := packet.Details()
	var errPointer unsafe.Pointer
	var wg sync.WaitGroup
	tree := broker.getSubscribeTree()
	for _, subMaps := range tree.MatchSubscribers(packet.TopicName) {
		for _, sub := range subMaps {
			wg.Add(1)
			packet.Qos = byte(minQos(int32(detail.Qos), sub.Qos()))
			sub.writePacket(packet, func(err error) {
				if err != nil {
					log.Warn(err)
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

func (broker *Broker) handleSubscribePacket(sessionItem *proto.MQTTSessionItem,
	packet *packets.SubscribePacket) error {
	event := proto.SubscribeEvent{
		SessionId:      sessionItem.SessionId,
		Qos0StreamInfo: sessionItem.Qos0StreamInfo,
		Qos1StreamInfo: sessionItem.Qos1StreamInfo,
		Topic:          map[string]int32{},
	}
	for index, topic := range packet.Topics {
		qos := packet.Qoss[index]
		event.Topic[topic] = int32(qos)
	}
	return broker.sendEvent(&event)
}

func (broker *Broker) handleRetainPacket(packet *packets.PublishPacket) error {
	var buffer bytes.Buffer
	_ = packet.Write(&buffer)
	event := proto.RetainMessageEvent{
		Data: buffer.Bytes(),
	}
	return broker.sendEvent(&event)
}

func (broker *Broker) checkpoint(clone *TopicTree, offset int64) error {
	err := broker.snapshot.WriteSnapshot(SnapshotHeader{
		TS:     time.Now(),
		Offset: offset,
	}, clone, broker.metaTree)
	if err != nil {
		log.Error(err)
		return err
	}
	if err := broker.eventReader.commitReadOffset(offset); err != nil {
		log.Error(err)
	}
	return nil
}

func (broker *Broker) handleClientStatusChange(sessionID int64, offline proto.ClientStatusChangeEvent_Status) error {
	event := &proto.ClientStatusChangeEvent{
		SessionID: sessionID,
		Status:    offline,
		BrokerId:  broker.BrokerId,
	}
	return broker.sendEvent(event)
}

func (broker *Broker) handleClientStatusChangeEvent(event *proto.ClientStatusChangeEvent) {
	log.WithField("event", event).Info("handleClientStatusChangeEvent")
	item := broker.metaTree.Get(&subscriberStatus{
		sessionID: event.SessionID,
		status:    &event.Status,
	})
	if item == nil {
		//copy on write
		metaTree := broker.metaTree.Clone()
		metaTree.ReplaceOrInsert(&subscriberStatus{
			sessionID: event.SessionID,
			brokerId:  event.BrokerId,
			status:    &event.Status,
		})
		broker.metaTree = metaTree
	} else {
		atomic.StoreInt32((*int32)(item.(*subscriberStatus).status), int32(event.Status))
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func (broker *Broker) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	conn, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		log.Errorf("%+v\n", err)
		return
	}
	broker.handleConnection(newWSConn(conn))
}
