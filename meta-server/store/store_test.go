package store

import (
	"context"
	"fmt"
	"github.com/akzj/mmdb"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"testing"
	"time"
	"unsafe"
)

func openStore(t *testing.T) (*Store, func()) {
	logger := logrus.New()
	store := OpenStore(mmdb.DefaultOptions(), logger)
	if store == nil {
		t.Fatalf("OpenStore failed")
	}
	return store, func() {
		if err := store.db.Close(context.Background()); err != nil {
			panic(err.Error())
		}
		if err := os.RemoveAll(mmdb.DefaultOptions().JournalDir); err != nil {
			t.Fatalf(err.Error())
		}
		if err := os.RemoveAll(mmdb.DefaultOptions().SnapshotDir); err != nil {
			t.Fatalf(err.Error())
		}
	}
}

func TestStoreStreamServer(t *testing.T) {
	store, release := openStore(t)
	defer release()

	for i := 0; i < 100; i++ {
		item := &StreamServerInfoItem{
			Base: &ServerInfoBase{
				Leader: false,
				Addr:   "127.0.0.1",
			},
		}
		if _, err := store.AddStreamServer(item); err != nil {
			t.Fatalf("%d", i)
		}
	}
	items, err := store.ListStreamServer()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 100 {
		t.Fatal(items)
	}
	store.db.Close(context.Background())

	{
		store, release := openStore(t)
		defer release()
		items, err := store.ListStreamServer()
		if err != nil {
			t.Fatal(err)
		}
		if len(items) != 100 {
			t.Fatal(items)
		}

		for _, item := range items {
			if err := store.DeleteStreamServer(item); err != nil {
				t.Fatalf(err.Error())
			}
		}

		items, err = store.ListStreamServer()
		if err != nil {
			t.Fatal(err)
		}
		if len(items) != 0 {
			t.Fatal(items)
		}
	}
}

func TestStoreOffset(t *testing.T) {

	fmt.Println(unsafe.Sizeof(SSOffsetItem{}))

	store, release := openStore(t)
	defer release()

	var items []*SSOffsetItem
	for i := 0; i < 100; i++ {
		items = append(items, &SSOffsetItem{
			SessionId: 0,
			StreamId:  int64(i),
			Offset:    int64(i),
		})
	}

	if err := store.SetOffSet(items); err != nil {
		t.Fatal(err)
	}

	for _, item := range items {
		offsetItem, err := store.GetOffset(item.SessionId, item.StreamId)
		if err != nil {
			t.Fatal(err)
		}
		if offsetItem != item {
			t.Fatal("get off set failed")
		}
	}
	if err := store.db.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	{
		store, release := openStore(t)
		defer release()
		for _, item := range items {
			offsetItem, err := store.GetOffset(item.SessionId, item.StreamId)
			if err != nil {
				t.Fatal(err)
			}
			if offsetItem == item {
				t.Fatal("get off set failed")
			}
			if offsetItem.Offset != item.Offset {
				t.Fatal("get offset failed")
			}
			delOffset, err := store.DelOffset(item.SessionId, item.StreamId)
			if err != nil {
				t.Fatal(err)
			}
			if delOffset.SessionId != item.SessionId || delOffset.StreamId != item.StreamId {
				t.Fatal(delOffset)
			}
		}

		SsOffsetItems, err := store.GetOffsets()
		if err != nil {
			t.Fatal(err)
		}
		if SsOffsetItems != nil {
			t.Fatal(SsOffsetItems)
		}
	}
}

func TestStore_CreateStream(t *testing.T) {
	store, release := openStore(t)
	defer release()

	for i := 0; i < 100; i++ {
		streamInfoItem, _, err := store.CreateStream("hello" + strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		if streamInfoItem.StreamId != int64(i+1) {
			t.Fatal(streamInfoItem)
		}
	}
	if _, create, err := store.CreateStream("hello1"); err != nil {
		t.Fatal(err)
	} else if create {
		t.Fatal("create must be false")
	}
}

func TestInsertStreamServerHeartbeatItem(t *testing.T) {
	store, release := openStore(t)
	defer release()

	if _, err := store.AddStreamServer(&StreamServerInfoItem{Base: &ServerInfoBase{Id: 1}}); err != nil {
		t.Fatal(err)
	}
	err := store.InsertStreamServerHeartbeatItem(&StreamServerHeartbeatItem{
		Base: &ServerInfoBase{
			Id:     1,
			Leader: false,
			Addr:   "127.0.0.1",
		},
		Timestamp: &timestamp.Timestamp{
			Seconds: time.Now().Unix(),
			Nanos:   0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	item, err := store.GetStreamServerHeartbeatItem(1)
	if err != nil {
		t.Fatal(err)
	}
	if item == nil {
		t.Fatal("no find")
	}
}
