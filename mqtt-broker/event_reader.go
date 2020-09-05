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
const MaxEventSize = 1024 * 1024

type EventReader struct {
	ctx       context.Context
	sessionID int64 //serverID
	session   client.StreamSession
	client    client.Client
	callback  eventCallback
	reader    client.StreamReader
}

type EventWithOffset struct {
	event  proto.Message
	offset int64
}

type eventCallback func(message EventWithOffset)

func newEventReader(sessionID int64, client client.Client, callback eventCallback) (*EventReader, error) {
	ctx := context.Background()
	_, err := client.GetOrCreateStream(ctx, MQTTEventStream)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sess, err := client.NewStreamSession(ctx, sessionID, MQTTEventStream)
	if err != nil {
		return nil, err
	}
	reader, err := sess.NewReader()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	offset, err := sess.GetReadOffset()
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
	return &EventReader{
		ctx:       ctx,
		sessionID: sessionID,
		session:   sess,
		client:    client,
		callback:  callback,
		reader:    reader,
	}, nil
}

func (watcher *EventReader) handleEvent(event EventWithOffset) {
	watcher.callback(event)
}

func (watcher *EventReader) readEventLoop() {
	offset := watcher.reader.Offset()
	resetReader := func() {
		if _, err := watcher.reader.Seek(offset, io.SeekStart); err != nil {
			log.Error(err)
			time.Sleep(time.Second)
		}
	}
	for {
		var length int32
		if err := binary.Read(watcher.reader, binary.BigEndian, &length); err != nil {
			log.Error(err.Error())
			resetReader()
			continue
		}
		if length > MaxEventSize {
			log.WithField("length", length).Panic("watcher.reader event length error")
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(watcher.reader, data); err != nil {
			log.Error(err)
			resetReader()
			continue
		}
		var event Event
		if err := proto.Unmarshal(data, &event); err != nil {
			log.Panic(err)
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
			log.Panic(err)
		}
		offset = watcher.reader.Offset()
		watcher.handleEvent(EventWithOffset{
			event:  message,
			offset: offset,
		})
	}
}

func (watcher *EventReader) commitReadOffset(offset int64) error {
	return watcher.session.SetReadOffset(offset)
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
