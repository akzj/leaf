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

package store

import (
	"encoding/binary"
	"github.com/akzj/streamIO/pkg/mmdb"
	"github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/pkg/errors"
	"io"
	"math"
)

const (
	MetaDataItemType = 1 + iota
	StreamInfoItemType
	SSOffsetItemType
	StreamServerInfoItemType
	StreamServerHeartbeatItemType
	MQTTSessionItemType
)

type Item interface {
	mmdb.Item
	GetType() uint16
	UnmarshalBinary(data []byte) error
}

func UnmarshalItem(data []byte) (mmdb.Item, error) {
	if len(data) < 2 {
		return nil, io.ErrUnexpectedEOF
	}
	var item Item
	switch itemType := binary.BigEndian.Uint16(data); itemType {
	case MetaDataItemType:
		item = new(MetaDataItem)
	case StreamInfoItemType:
		item = new(StreamInfoItem)
	case SSOffsetItemType:
		item = new(SSOffsetItem)
	case StreamServerInfoItemType:
		item = new(StreamServerInfoItem)
	case StreamServerHeartbeatItemType:
		item = new(StreamServerHeartbeatItem)
	case MQTTSessionItemType:
		item = new(MQTTSessionItem)
	default:
		return nil, errors.Errorf("unknown type %d", itemType)
	}
	if err := item.UnmarshalBinary(data[2:]); err != nil {
		return nil, err
	}
	return item, nil
}

func MarshalItem(x Item) ([]byte, error) {
	var buffers = make([]byte, 2)
	binary.BigEndian.PutUint16(buffers, x.GetType())
	if data, err := proto.Marshal(x.(proto.Message)); err != nil {
		return nil, err
	} else {
		return append(buffers, data...), nil
	}
}

func NewStreamInfoItem(ID int64, name string) *StreamInfoItem {
	return &StreamInfoItem{
		Name:     name,
		StreamId: ID,
	}
}

func (x *StreamInfoItem) Less(other btree.Item) bool {
	if x.GetType() != other.(Item).GetType() {
		return x.GetType() < other.(Item).GetType()
	}
	return x.Name < other.(*StreamInfoItem).Name
}

func (x *StreamInfoItem) MarshalBinary() (data []byte, err error) {
	return MarshalItem(x)
}

func (x *StreamInfoItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *StreamInfoItem) GetType() uint16 {
	return StreamInfoItemType
}

//MetaDataItem

var metaDataItemKey = &MetaDataItem{Key: 1}

func (x *MetaDataItem) Less(other btree.Item) bool {
	if x.GetType() != other.(Item).GetType() {
		return x.GetType() < other.(Item).GetType()
	}
	return x.Key < other.(*MetaDataItem).Key
}

func (x *MetaDataItem) MarshalBinary() (data []byte, err error) {
	return MarshalItem(x)
}

func (x *MetaDataItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *MetaDataItem) GetType() uint16 {
	return MetaDataItemType
}

//SSOffsetItem

func (x *SSOffsetItem) Less(other btree.Item) bool {
	if x.GetType() != other.(Item).GetType() {
		return x.GetType() < other.(Item).GetType()
	}
	if x.SessionId != other.(*SSOffsetItem).SessionId {
		return x.SessionId < other.(*SSOffsetItem).SessionId
	}
	return x.StreamId < other.(*SSOffsetItem).StreamId
}

func (x *SSOffsetItem) MarshalBinary() (data []byte, err error) {
	return MarshalItem(x)
}

func (x *SSOffsetItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *SSOffsetItem) GetType() uint16 {
	return SSOffsetItemType
}

//StreamServerInfoItem

func (x *StreamServerInfoItem) Less(other btree.Item) bool {
	if x.GetType() != other.(Item).GetType() {
		return x.GetType() < other.(Item).GetType()
	}
	return x.Base.Id < other.(*StreamServerInfoItem).Base.Id
}

func (x *StreamServerInfoItem) MarshalBinary() (data []byte, err error) {
	return MarshalItem(x)
}

func (x *StreamServerInfoItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *StreamServerInfoItem) GetType() uint16 {
	return StreamServerInfoItemType
}

//StreamServerHeartbeatItem

var (
	streamServerHeartbeatItemKeyMin = &StreamServerHeartbeatItem{
		Base: &ServerInfoBase{Id: 0},
	}
	streamServerHeartbeatItemKeyMax = &StreamServerHeartbeatItem{
		Base: &ServerInfoBase{Id: math.MaxInt64},
	}
)

func (x *StreamServerHeartbeatItem) Less(other btree.Item) bool {
	if x.GetType() != other.(Item).GetType() {
		return x.GetType() < other.(Item).GetType()
	}
	return x.Base.Id < other.(*StreamServerHeartbeatItem).Base.Id
}

func (x *StreamServerHeartbeatItem) MarshalBinary() (data []byte, err error) {
	return MarshalItem(x)
}

func (x *StreamServerHeartbeatItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *StreamServerHeartbeatItem) GetType() uint16 {
	return StreamServerHeartbeatItemType
}

//MQTTSessionItem

func (x *MQTTSessionItem) Less(other btree.Item) bool {
	if x.GetType() != other.(Item).GetType() {
		return x.GetType() < other.(Item).GetType()
	}
	return x.ClientIdentifier < other.(*MQTTSessionItem).ClientIdentifier
}

func (x *MQTTSessionItem) MarshalBinary() (data []byte, err error) {
	return MarshalItem(x)
}

func (x *MQTTSessionItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *MQTTSessionItem) GetType() uint16 {
	return MQTTSessionItemType
}

func (x *MQTTSessionItem) Clone() *MQTTSessionItem {
	clone := new(MQTTSessionItem)
	clone.Qos0StreamInfo = &StreamInfoItem{
		StreamId:       x.Qos0StreamInfo.StreamId,
		Name:           x.Qos0StreamInfo.Name,
		StreamServerId: x.Qos0StreamInfo.StreamServerId,
	}
	clone.Qos1StreamInfo = &StreamInfoItem{
		StreamId:       x.Qos1StreamInfo.StreamId,
		Name:           x.Qos1StreamInfo.Name,
		StreamServerId: x.Qos1StreamInfo.StreamServerId,
	}
	clone.CreateTs = x.CreateTs
	clone.AccessTs = x.AccessTs
	clone.SessionId = x.SessionId
	clone.ClientIdentifier = x.ClientIdentifier
	clone.Topics = map[string]int32{}
	for topic, qos := range x.Topics {
		clone.Topics[topic] = qos
	}
	return clone
}
