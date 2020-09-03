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

const MQTTEventStream = "$streamIO$mqtt-event"
const MaxEventSize = 1024 * 64

type EventWatcher struct {
	sessionID     int64 //serverID
	session       client.StreamSession
	client        client.Client
	ctx           context.Context
	eventCallback func(message proto.Message)
}

func newEventWatcher(reader client.Client) *EventWatcher {
	return &EventWatcher{

	}
}

func (watcher *EventWatcher) init() error {
	if _, err := watcher.client.GetOrCreateStream(watcher.ctx, MQTTEventStream); err != nil {
		log.Error(err.Error())
		return err
	}
	session, err := watcher.client.NewStreamSession(watcher.ctx, watcher.sessionID, MQTTEventStream)
	if err != nil {
		log.Error(err)
		return err
	}
	watcher.session = session
	return nil
}

func (watcher *EventWatcher) getReader() client.ReadSeekCloser {
	reader, err := watcher.session.NewReader()
	if err != nil {
		log.Error(err)
		return nil
	}
	return reader
}

func (watcher *EventWatcher) readEvent(reader io.Reader) error {
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

func (watcher *EventWatcher) handleEvent(event proto.Message) {
	watcher.eventCallback(event)
}

func (watcher *EventWatcher) readEventLoop() {
	reader := watcher.getReader()
	if reader == nil {
		log.Fatal("EventWatcher getReader failed")
	}
	for {
		offset, err := watcher.session.GetReadOffset()
		if err != nil {
			log.Fatal(err)
		}
		if _, err := reader.Seek(offset, io.SeekStart); err != nil {
			log.Fatal(err)
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
