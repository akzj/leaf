package store

import (
	"github.com/akzj/mmdb"
	"github.com/pkg/errors"
	"math"
)

type Store struct {
	db mmdb.DB
}

func (store *Store) CreateStream(name string) (*StreamInfoItem, error) {
	var exist = false
	streamInfoItem := NewStreamInfoItem(0, name)
	err := store.db.Update(func(tx mmdb.Transaction) error {
		if tx.Get(streamInfoItem) != nil {
			exist = true
			return nil
		}
		var metaDataItem *MetaDataItem
		item := tx.Get(metaDataItemKey)
		if item == nil {
			metaDataItem = new(MetaDataItem)
			metaDataItem.NextStreamId = 1
		} else {
			metaDataItem = item.(*MetaDataItem)
		}
		streamInfoItem.StreamId = metaDataItem.NextStreamId
		metaDataItem.NextStreamId++

		tx.ReplaceOrInsert(streamInfoItem)
		tx.ReplaceOrInsert(metaDataItem)
		return nil
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if exist {
		return nil, errors.Errorf("stream name exist %s", name)
	}
	return streamInfoItem, nil
}

func (store *Store) GetStream(name string) (*StreamInfoItem, error) {
	var streamInfoItem *StreamInfoItem
	err := store.db.View(func(tx mmdb.Transaction) error {
		if item := tx.Get(NewStreamInfoItem(0, name)); item != nil {
			streamInfoItem = item.(*StreamInfoItem)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return streamInfoItem, nil
}

func (store *Store) SetOffSet(items []*SSOffsetItem) error {
	return store.db.Update(func(tx mmdb.Transaction) error {
		for _, item := range items {
			tx.ReplaceOrInsert(item)
		}
		return nil
	})
}

func (store *Store) GetOffset(SessionId int64, StreamId int64) (*SSOffsetItem, error) {
	var ssOffsetItem *SSOffsetItem
	err := store.db.View(func(tx mmdb.Transaction) error {
		item := tx.Get(&SSOffsetItem{SessionId: SessionId, StreamId: StreamId})
		if item != nil {
			ssOffsetItem = item.(*SSOffsetItem)
		}
		return nil
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return ssOffsetItem, nil
}

var streamServerInfoItemBegin = &StreamServerInfoItem{}
var streamServerInfoItemEnd = &StreamServerInfoItem{Base: &ServerInfoBase{
	Id: math.MaxUint32,
}}

func (store *Store) ListStreamServer() ([]*StreamServerInfoItem, error) {
	var streamServerInfoItems []*StreamServerInfoItem
	err := store.db.View(func(tx mmdb.Transaction) error {
		tx.AscendRange(streamServerInfoItemBegin, streamServerInfoItemEnd, func(item mmdb.Item) bool {
			streamServerInfoItems = append(streamServerInfoItems, item.(*StreamServerInfoItem))
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return streamServerInfoItems, nil
}

func (store *Store) AddStreamServer(item *StreamServerInfoItem) (*StreamServerInfoItem, error) {
	err := store.db.Update(func(tx mmdb.Transaction) error {
		var lastItem mmdb.Item
		tx.AscendRange(streamServerInfoItemBegin, streamServerInfoItemEnd, func(item mmdb.Item) bool {
			lastItem = item
			return true
		})
		if lastItem == nil {
			item.Base.Id = 1
		} else {
			item.Base.Id = 1 + lastItem.(*StreamServerInfoItem).Base.Id
		}
		tx.ReplaceOrInsert(item)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (store *Store) DeleteStreamServer(item *StreamServerInfoItem) error {
	err := store.db.Update(func(tx mmdb.Transaction) error {
		tx.Delete(item)
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (store *Store) InsertStreamServerHeartbeatItem(item *StreamServerHeartbeatItem) error {
	var find = false
	err := store.db.Update(func(tx mmdb.Transaction) error {
		if tx.Get(&StreamServerInfoItem{Base: item.ServerInfoBase}) == nil {
			return nil
		}
		tx.ReplaceOrInsert(item)
		find = true
		return nil
	})
	if err != nil {
		return err
	}
	if find == false {
		err = errors.Errorf("no find stream server ID %d", item.ServerInfoBase.Id)
	}
	return err
}
