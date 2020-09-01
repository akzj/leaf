package store

import (
	"github.com/akzj/mmdb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"math"
	"sync"
)

type Store struct {
	db  mmdb.DB

	//protect for metadata
	metaDataItemLocker sync.Mutex
}

func OpenStore(options mmdb.Options) *Store {
	db, err := mmdb.OpenDB(options.WithUnmarshalBinary(UnmarshalItem))
	if err != nil {
		log.Panic(err)
	}

	if err := db.Update(func(tx mmdb.Transaction) error {
		item := tx.Get(metaDataItemKey)
		if item == nil {
			tx.ReplaceOrInsert(&MetaDataItem{NextStreamId: 1, Key: 1})
		}
		return nil
	}); err != nil {
		log.Panic(err)
	}
	return &Store{
		db:  db,
	}
}

func (store *Store) CreateStream(name string) (item *StreamInfoItem, create bool, err error) {
	store.metaDataItemLocker.Lock()
	defer store.metaDataItemLocker.Unlock()
	var exist = false
	streamInfoItem := NewStreamInfoItem(0, name)
	err = store.db.Update(func(tx mmdb.Transaction) error {
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
		log.Warn(err)
		return nil, false, errors.WithStack(err)
	}
	if exist {
		return streamInfoItem, false, nil
	}
	return streamInfoItem, true, nil
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
		log.Warn(err)
		return nil, err
	}
	return streamInfoItem, nil
}

func (store *Store) SetOffSet(items []*SSOffsetItem) error {
	err := store.db.Update(func(tx mmdb.Transaction) error {
		for _, item := range items {
			tx.ReplaceOrInsert(item)
		}
		return nil
	})
	if err != nil {
		log.Warn(err)
		return errors.WithStack(err)
	}
	return nil
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
		log.Warn(err)
		return nil, errors.WithStack(err)
	}
	return ssOffsetItem, nil
}

func (store *Store) DelOffset(sessionID int64, streamID int64) (*SSOffsetItem, error) {
	var item mmdb.Item
	err := store.db.Update(func(tx mmdb.Transaction) error {
		item = tx.Delete(&SSOffsetItem{SessionId: sessionID, StreamId: streamID})
		return nil
	})
	if err != nil {
		log.Warn(err)
		return nil, err
	}
	if item == nil {
		return nil, nil
	}
	return item.(*SSOffsetItem), nil
}

func (store *Store) GetOffsets() ([]*SSOffsetItem, error) {
	var items []*SSOffsetItem
	err := store.db.View(func(tx mmdb.Transaction) error {
		tx.AscendRange(&SSOffsetItem{SessionId: 0}, &SSOffsetItem{SessionId: math.MaxInt64}, func(item mmdb.Item) bool {
			items = append(items, item.(*SSOffsetItem))
			return true
		})
		return nil
	})
	if err != nil {
		log.Warn(err)
		return nil, err
	}
	return items, nil
}

var streamServerInfoItemBegin = &StreamServerInfoItem{
	Base: &ServerInfoBase{
		Id: 0,
	},
}
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
		log.Warn(err)
		return nil, err
	}
	return streamServerInfoItems, nil
}

func (store *Store) GetStreamServerInfo(id int64) (*StreamServerInfoItem, error) {
	var item mmdb.Item
	err := store.db.View(func(tx mmdb.Transaction) error {
		item = tx.Get(&StreamServerInfoItem{
			Base: &ServerInfoBase{Id: id},
		})
		return nil
	})
	if err != nil {
		log.Warning(err)
		return nil, err
	}
	if item == nil {
		return nil, nil
	}
	return item.(*StreamServerInfoItem), err
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
		log.Warn(err)
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
		log.Warn(err)
		return errors.WithStack(err)
	}
	return nil
}

func (store *Store) InsertStreamServerHeartbeatItem(item *StreamServerHeartbeatItem) error {
	var find = false
	err := store.db.Update(func(tx mmdb.Transaction) error {
		if tx.Get(&StreamServerInfoItem{Base: item.Base}) == nil {
			return nil
		}
		tx.ReplaceOrInsert(item)
		find = true
		return nil
	})
	if err != nil {
		log.Warn(err)
		return err
	}
	if find == false {
		err = errors.Errorf("no find stream server ID %d", item.Base.Id)
	}
	return err
}

func (store *Store) GetStreamServerHeartbeatItem(ID int64) (*StreamServerHeartbeatItem, error) {
	var item mmdb.Item
	err := store.db.View(func(tx mmdb.Transaction) error {
		item = tx.Get(&StreamServerHeartbeatItem{Base: &ServerInfoBase{Id: ID}})
		return nil
	})
	if err != nil {
		log.Warning(err)
		return nil, err
	}
	if item == nil {
		return nil, nil
	}
	return item.(*StreamServerHeartbeatItem), nil
}
