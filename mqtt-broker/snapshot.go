package mqtt_broker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const snapFilename = "streamIO.mqtt.snap"
const snapExtTmp = snapFilename + ".tmp"

type Snapshot struct {
	path string
}

type SnapshotHeader struct {
	TS     time.Time `json:"ts"`
	Offset int64     `json:"offset"`
}

func NewSnapshot(path string) *Snapshot {
	return &Snapshot{
		path: path,
	}
}

func (s *Snapshot) reloadSnapshot(broker *Broker) error {
	filename := filepath.Join(s.path, snapFilename)
	if _, err := os.Stat(filename); err != nil {
		log.Infof("snapshot %s no exist", filename)
		return nil
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Error(err.Error())
		return errors.WithStack(err)
	}
	reader := bufio.NewReader(file)
	var length int32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return errors.WithStack(err)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		log.Error(err)
		return err
	}
	for {
		var dataLen int32
		if err := binary.Read(reader, binary.BigEndian, &dataLen); err != nil {
			if err == io.EOF {
				return nil
			}
			log.Error(err)
			return err
		}
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(reader, data); err != nil {
			log.Error(err)
			return err
		}
		var event Event
		if err := proto.Unmarshal(data, &event); err != nil {
			log.Error(err)
			return err
		}
		switch event.Type {
		case Event_SubscribeEvent:
			var subEvent SubscribeEvent
			if err := proto.Unmarshal(event.Data, &subEvent); err != nil {
				log.Error(err)
				return err
			}
			broker.insertSubscriber2Tree(broker.getSubscribeTree(), &subEvent)
		case Event_RetainMessage:
			var message RetainMessage
			if err := proto.Unmarshal(event.Data, &message); err != nil {
				log.Error(err)
				return err
			}
			if err := broker.insertRetainMessage2Tree(broker.getSubscribeTree(), &message); err != nil {
				return err
			}
		}
	}
}

func (s *Snapshot) WriteSnapshot(header SnapshotHeader, tree *Tree) error {
	filename := filepath.Join(s.path, time.Now().String()+snapExtTmp)
	file, err := os.Create(filename)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	writer := bufio.NewWriterSize(file, 1024*1024)
	data, _ := json.Marshal(header)
	if err := binary.Write(writer, binary.BigEndian, int32(len(data))); err != nil {
		log.Error(err.Error())
		return err
	}
	if _, err := writer.Write(data); err != nil {
		log.Error(err.Error())
		return err
	}
	tree.Walk(func(path string, subscribers map[int64]Subscriber) bool {
		for _, iter := range subscribers {
			sub := iter.(*subscriber)
			var subEvent = &SubscribeEvent{
				StreamId:       sub.streamID,
				SessionId:      sub.streamID,
				StreamServerId: sub.streamServerID,
				Topic:          map[string]int32{iter.Topic(): iter.Qos()},
			}
			data, err = proto.Marshal(subEvent)
			if err != nil {
				log.Fatal(err)
			}
			data, err = proto.Marshal(&Event{Data: data, Type: Event_SubscribeEvent})
			if err != nil {
				log.Fatal(err)
			}
			if err = binary.Write(writer, binary.BigEndian, int32(len(data))); err != nil {
				log.Error(err)
				return false
			}
			if _, err = writer.Write(data); err != nil {
				log.Error(err)
				return false
			}
		}
		return true
	})
	if err != nil {
		return err
	}

	tree.RangeRetainMessage(func(packet *packets.PublishPacket) bool {
		var buffer bytes.Buffer
		if err = packet.Write(&buffer); err != nil {
			log.Fatal(err)
		}
		data, err = proto.Marshal(&Event{Data: buffer.Bytes(), Type: Event_RetainMessage})
		if err != nil {
			log.Fatal(err.Error())
		}
		if err = binary.Write(writer, binary.BigEndian, int32(len(data))); err != nil {
			log.Error(err.Error())
			return false
		}
		if _, err = writer.Write(data); err != nil {
			log.Error(err)
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	if err = writer.Flush(); err != nil {
		log.Error(err)
		return err
	}
	if err := file.Close(); err != nil {
		log.Error(err.Error())
		return err
	}
	if err := os.Rename(filename, strings.ReplaceAll(filename, snapExtTmp, snapFilename)); err != nil {
		log.Error(err.Error())
		return err
	}
	return nil
}
