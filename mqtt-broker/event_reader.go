package mqtt_broker

import (
	"context"
	"encoding/binary"
	"github.com/akzj/streamIO/client"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"time"
)

const MQTTEventStream = "$streamIO-mqtt-broker-event"
const MaxEventSize = 1024 * 64

type EventReader struct {
	ctx       context.Context
	sessionID int64 //serverID
	session   client.StreamSession
	client    client.Client
	callback  eventCallback
}

type eventCallback func(message proto.Message)

func newEventWatcher(sessionID int64, client client.Client, callback eventCallback) (*EventReader, error) {
	ctx := context.Background()
	_, err := client.GetOrCreateStream(ctx, MQTTEventStream)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sess, err := client.NewStreamSession(ctx, sessionID, MQTTEventStream)
	if err != nil {
		return nil, err
	}
	return &EventReader{
		ctx:       ctx,
		sessionID: sessionID,
		session:   sess,
		client:    client,
		callback:  callback,
	}, nil
}

func (watcher *EventReader) getReader() client.StreamReader {
	reader, err := watcher.session.NewReader()
	if err != nil {
		log.Error(err)
		return nil
	}
	return reader
}

func (watcher *EventReader) readEvent(reader io.Reader) error {
	for {
		var length int32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			log.Error(err.Error())
			return err
		}
		if length > MaxEventSize {
			log.WithField("length", length).Errorf("length > MaxEventSize")
			return errors.New("event length error")
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}
		var event Event
		if err := proto.Unmarshal(data, &event); err != nil {
			log.Errorf(err.Error())
			return err
		}
		var message proto.Message
		switch event.Type {
		case Event_SubscribeEvent:
			message = &SubscribeEvent{}
		case Event_UnSubscribeEvent:
			message = &UnSubscribeEvent{}
		case Event_RetainMessage:
			message = &RetainMessage{}
		}
		if err := proto.Unmarshal(event.Data, message); err != nil {
			log.Error(err.Error())
			return err
		}
		watcher.handleEvent(message)
	}
}

func (watcher *EventReader) handleEvent(event proto.Message) {
	watcher.callback(event)
}

func (watcher *EventReader) readEventLoop() {
	reader := watcher.getReader()
	if reader == nil {
		log.Fatal("EventReader getReader failed")
	}
	for {
		offset, err := watcher.session.GetReadOffset()
		if err != nil {
			log.Error(err)
			if sleep(watcher.ctx) != nil {
				return
			}
			continue
		}
		if _, err := reader.Seek(offset, io.SeekStart); err != nil {
			log.Error(err)
			if sleep(watcher.ctx) != nil {
				return
			}
			continue
		}
		if err := watcher.readEvent(reader); err != nil {
			log.Error(err)
		}
		_ = reader.Close()
		if sleep(watcher.ctx) != nil {
			return
		}
	}
}

func sleep(ctx context.Context, duration ...time.Duration) error {
	var d = time.Second
	if duration != nil {
		d = duration[0]
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
	}
	return nil
}
