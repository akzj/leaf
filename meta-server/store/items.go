package store

import (
	"github.com/akzj/mmdb"
	"github.com/golang/protobuf/proto"
	"github.com/google/btree"
)

const (
	MetaDataItemType = 1 + iota
	StreamInfoItemType
	SSOffsetItemType
	StreamServerInfoItemType
	StreamServerHeartbeatItemType
)

type Item interface {
	mmdb.Item
	GetType() int
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
	return proto.Marshal(x)
}

func (x *StreamInfoItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *StreamInfoItem) GetType() int {
	return StreamInfoItemType
}

//MetaDataItem

var metaDataItemKey = &MetaDataItem{}

func (x *MetaDataItem) Less(other btree.Item) bool {
	if x.GetType() != other.(Item).GetType() {
		return x.GetType() < other.(Item).GetType()
	}
	panic("MetaDataItem is only one")
}

func (x *MetaDataItem) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(x)
}

func (x *MetaDataItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *MetaDataItem) GetType() int {
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
	return proto.Marshal(x)
}

func (x *SSOffsetItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *SSOffsetItem) GetType() int {
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
	return proto.Marshal(x)
}

func (x *StreamServerInfoItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *StreamServerInfoItem) GetType() int {
	return StreamServerInfoItemType
}

//StreamServerHeartbeatItem

func (x *StreamServerHeartbeatItem) Less(other btree.Item) bool {
	if x.GetType() != other.(Item).GetType() {
		return x.GetType() < other.(Item).GetType()
	}
	return x.ServerInfoBase.Id < other.(*StreamServerHeartbeatItem).ServerInfoBase.Id
}

func (x *StreamServerHeartbeatItem) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(x)
}

func (x *StreamServerHeartbeatItem) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, x)
}

func (x *StreamServerHeartbeatItem) GetType() int {
	return StreamServerHeartbeatItemType
}