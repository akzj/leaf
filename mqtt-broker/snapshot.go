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
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/akzj/streamIO/proto"
	"github.com/eclipse/paho.mqtt.golang/packets"
	pproto "github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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
		var event proto.Event
		if err := pproto.Unmarshal(data, &event); err != nil {
			log.Error(err)
			return err
		}
		switch event.Type {
		case proto.Event_SubscribeEvent:
			var subEvent proto.SubscribeEvent
			if err := pproto.Unmarshal(event.Data, &subEvent); err != nil {
				log.Error(err)
				return err
			}
			broker.insertSubscriber2Tree(broker.getSubscribeTree(), &subEvent)
		case proto.Event_RetainMessageEvent:
			var message proto.RetainMessageEvent
			if err := pproto.Unmarshal(event.Data, &message); err != nil {
				log.Error(err)
				return err
			}
			if err := broker.insertRetainMessage2Tree(broker.getSubscribeTree(), &message); err != nil {
				return err
			}
		case proto.Event_ClientStatusChangeEvent:
			var message proto.ClientStatusChangeEvent
			if err := pproto.Unmarshal(event.Data, &message); err != nil {
				log.Error(err)
				return err
			}
			broker.metaTree.ReplaceOrInsert(&subscriberStatus{
				sessionID: message.SessionID,
				brokerId:  message.BrokerId,
				status:    &message.Status,
			})
		}
	}
}

func (s *Snapshot) WriteSnapshot(header SnapshotHeader, topicTree *TopicTree, metaTree *btree.BTree) error {
	_ = os.MkdirAll(s.path, 0777)
	filename := filepath.Join(s.path, snapExtTmp)
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

	writerEvent := func(event *proto.Event) error {
		data, err = pproto.Marshal(event)
		if err != nil {
			log.Fatal(err)
		}
		if err = binary.Write(writer, binary.BigEndian, int32(len(data))); err != nil {
			return err
		}
		if _, err = writer.Write(data); err != nil {
			return err
		}
		return nil
	}
	topicTree.Walk(func(path string, subscribers map[int64]Subscriber) bool {
		for _, iter := range subscribers {
			sub := iter.(*subscriber)
			var subEvent = &proto.SubscribeEvent{
				SessionId: sub.sessionID,
				Topic:     map[string]int32{iter.Topic(): iter.Qos()},
			}
			if iter.Qos() == 0 {
				subEvent.Qos0StreamInfo = sub.streamInfo
			} else {
				subEvent.Qos1StreamInfo = sub.streamInfo
			}
			data, err = pproto.Marshal(subEvent)
			if err != nil {
				log.Fatal(err)
			}
			if err := writerEvent(&proto.Event{Data: data, Type: proto.Event_SubscribeEvent}); err != nil {
				log.Errorf("%+v\n", err)
				return false
			}
		}
		return true
	})
	if err != nil {
		return err
	}

	topicTree.RangeRetainMessage(func(packet *packets.PublishPacket) bool {
		var message proto.RetainMessageEvent
		var buffer bytes.Buffer
		if err = packet.Write(&buffer); err != nil {
			log.Fatal(err)
		}
		message.Data = buffer.Bytes()
		data, _ := pproto.Marshal(&message)
		if err = writerEvent(&proto.Event{Data: data, Type: proto.Event_RetainMessageEvent}); err != nil {
			log.Error(err.Error())
			return false
		}
		return true
	})
	if err != nil {
		return err
	}

	metaTree.Descend(func(item btree.Item) bool {
		var event proto.Event
		switch obj := item.(type) {
		case *subscriberStatus:
			var err error
			status := proto.ClientStatusChangeEvent_Status(atomic.LoadInt32((*int32)(obj.status)))
			event.Type = proto.Event_ClientStatusChangeEvent
			event.Data, err = pproto.Marshal(&proto.ClientStatusChangeEvent{Status: status, SessionID: obj.sessionID})
			if err != nil {
				log.Fatal(err)
			}
		default:
			log.Fatalf("unknown %+v\n", obj)
		}
		if err = writerEvent(&event); err != nil {
			log.Error(err.Error())
			return false
		}
		return true
	})
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
