package mqtt_broker

import (
	"crypto/tls"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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
}

func (broker *Broker) Start() {
	broker.clientListenLoop()
}

func (broker *Broker) getSubscribeTree() *Tree {
	return (*Tree)(atomic.LoadPointer(&broker.tree))
}

func (broker *Broker) newListener() ([]net.Listener, error) {
	var listeners []net.Listener
	if broker.BindPort != 0 {
		listener, err := net.Listen("tcp", net.JoinHostPort(broker.HOST, strconv.Itoa(broker.BindPort)))
		if err != nil {
			log.WithField("broker.BindPort", broker.BindPort).Error(err)
			return nil, err
		}
		listeners = append(listeners, listener)
	}
	if broker.BindTLSPort != 0 {
		listener, err := tls.Listen("tcp", net.JoinHostPort(broker.HOST, strconv.Itoa(broker.BindPort)), broker.tlsConfig)
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
	//1 todo get mqtt client session from meta-server
	//2 reset message offset from stream-server
	//3 delete client session from meta-server
	//4 delete subscribe,and pub `unsubscribe-event` to mqtt-event-queue
	return nil
}

func (broker *Broker) getOrCreateSession(identifier string) (*session, error) {
	return nil, nil
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
	var sess *session
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
	sess, err = broker.getOrCreateSession(connectPacket.ClientIdentifier)
	if err != nil {
		connackPacket.ReturnCode = packets.ErrRefusedServerUnavailable
		connackPacket.Write(conn)
		logEntry.Error(err)
		return
	}
	connackPacket.SessionPresent = !sess.create
	sess.broker = broker
	sess.conn = conn
	sess.keepalive = connectPacket.Keepalive
	if sess.keepalive == 0 {
		sess.keepalive = broker.DefaultKeepalive
	}
	broker.sessReadLoop(sess)
}

func (broker *Broker) sessReadLoop(sess *session) {
	logEntry := log.WithField("remoteAddr", sess.conn.RemoteAddr().String())
	logEntry = logEntry.WithField("ClientIdentifier", sess.base.ClientIdentifier)
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
