package mqtt_broker

import (
	"bytes"
	"context"
	"crypto/tls"
	"github.com/akzj/block-queue"
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
	"time"
	"unsafe"
)

type Broker struct {
	Options
	tlsConfig *tls.Config

	sessionsLocker sync.Mutex
	sessions       map[string]*session

	tree unsafe.Pointer

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
}

func New(options Options) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	metaServerClient, err := client.NewMetaServiceClient(ctx, options.MetaServerAddr)
	if err != nil {
		return nil, err
	}
	cli := client.NewClient(metaServerClient)
	if _, err := cli.GetOrCreateStream(ctx, MQTTEventStream); err != nil {
		return nil, err
	}
	sess, err := cli.NewStreamSession(ctx, options.BrokerId, MQTTEventStream)
	if err != nil {
		return nil, err
	}
	eventWriter, err := sess.NewWriter()
	if err != nil {
		return nil, err
	}

	eventQueue := block_queue.NewQueue(128)
	eventWatcher, err := newEventReader(options.BrokerId, cli, func(message EventWithOffset) {
		eventQueue.Push(message)
	})
	if err != nil {
		return nil, err
	}

	broker := &Broker{
		Options:        options,
		tlsConfig:      nil,
		sessionsLocker: sync.Mutex{},
		sessions:       map[string]*session{},
		tree:           unsafe.Pointer(NewTree()),
		eventReader:    eventWatcher,
		eventQueue:     eventQueue,
		client:         cli,
		eventOffset:    0,
		treeChanges:    0,
		isCheckpoint:   0,
		eventWriter:    eventWriter,
		snapshot:       NewSnapshot(options.SnapshotPath),
		ctx:            ctx,
		cancel:         cancel,
	}

	return broker, nil
}

func (broker *Broker) Start() error {
	if err := broker.snapshot.reloadSnapshot(broker); err != nil {
		return err
	}
	broker.clientListenLoop()
	return nil
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
	go sess.readStreamLoop()
	sess.readConnLoop()
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
				log.Error("http: Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
		}
		go broker.handleConnection(conn)
	}
}

func (broker *Broker) clientListenLoop() error {
	listeners, err := broker.newListener()
	if err != nil {
		return err
	}
	for _, listener := range listeners {
		go broker.serve(listener)
	}
	return nil
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
	tree := broker.getSubscribeTree().Clone()
	broker.insertSubscriber2Tree(tree, event)
	broker.setSubscribeTree(tree)
}

func (broker *Broker) insertSubscriber2Tree(tree *Tree, event *SubscribeEvent) {
	subs, err := broker.newSubscriber(event)
	if err != nil {
		log.Error(err.Error())
		return
	}
	for _, sub := range subs {
		tree.Insert(sub)
	}
}

func (broker *Broker) handleUnSubscribeEvent(event *UnSubscribeEvent) {
	tree := broker.getSubscribeTree().Clone()
	for _, topic := range event.Topic {
		tree.Delete(&subscriber{topic: topic, streamID: event.StreamId})
	}
	broker.setSubscribeTree(tree)
}

func (broker *Broker) handleRetainMessage(event *RetainMessage) {
	tree := broker.getSubscribeTree().Clone()
	_ = broker.insertRetainMessage2Tree(tree, event)
	broker.setSubscribeTree(tree)
}
func (broker *Broker) insertRetainMessage2Tree(tree *Tree, event *RetainMessage) error {
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
			case *SubscribeEvent:
				broker.handleSubscribeEvent(event)
			case *UnSubscribeEvent:
				broker.handleUnSubscribeEvent(event)
			case *RetainMessage:
				broker.handleRetainMessage(event)
			default:
				log.Fatal("unknown event %+v", event)
			}
			broker.eventOffset = event.offset
			broker.treeChanges++
		}
		if broker.treeChanges > broker.SubTreeCheckpointEventSize {
			if atomic.CompareAndSwapInt32(&broker.isCheckpoint, 0, 1) == false {
				continue
			}
			clone := broker.getSubscribeTree().Clone()
			go func() {
				defer func() {
					atomic.StoreInt32(&broker.isCheckpoint, 0)
				}()
				broker.checkpoint(clone, broker.eventOffset)
				log.Infof("checkpoint success")
			}()
		}
	}
}

func (broker *Broker) getSnapshotFile() (io.WriteCloser, error) {
	return nil, nil
}

func (broker *Broker) handleUnSubscribePacket(MqttSessionItem *store.MQTTSessionItem,
	packet *packets.UnsubscribePacket) error {
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

func (broker *Broker) handleSubscribePacket(MqttSessionItem *store.MQTTSessionItem,
	packet *packets.SubscribePacket) error {

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

func (broker *Broker) handleRetainPacket(packet *packets.PublishPacket) error {
	var buffer bytes.Buffer
	_ = packet.Write(&buffer)
	event := RetainMessage{
		Data: buffer.Bytes(),
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

func (broker *Broker) checkpoint(clone *Tree, offset int64) error {
	err := broker.snapshot.WriteSnapshot(SnapshotHeader{
		TS:     time.Now(),
		Offset: offset,
	}, clone)
	if err != nil {
		log.Error(err)
		return err
	}
	if err := broker.eventReader.commitReadOffset(offset);err != nil{
		log.Error(err)
	}
	return nil
}
