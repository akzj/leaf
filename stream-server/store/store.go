package store

import (
	"context"
	"fmt"
	"github.com/akzj/sstore"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"io"
	"math"
)

type Store struct {
	sstore *sstore.SStore
}

type StreamStat struct {
	StreamID int64
	Begin    int64
	End      int64
}

func OpenStore(path string) (*Store, error) {
	sstore, err := sstore.Open(sstore.DefaultOptions(path))
	if err != nil {
		log.Warningf("sstore open %s failed %+v", path, err)
		return nil, err
	}
	return &Store{
		sstore: sstore,
	}, nil
}

func (store *Store) WriteRequest(request *proto.WriteStreamRequest, callback func(offset int64, err error)) {
	store.sstore.AsyncAppend(request.StreamId, request.Data, request.Offset, callback)
}

func (store *Store) ReadRequest(ctx context.Context, request *proto.ReadStreamRequest,
	callback func(offset int64, data []byte) error) error {
	reader, err := store.sstore.Reader(request.StreamId)
	if err != nil {
		log.WithField("streamID", request.StreamId).Warn(err)
		return err
	}

	if _, err := reader.Seek(request.Offset, io.SeekStart); err != nil {
		log.WithField("Offset", request.Offset).Warn(err)
		return err
	}
	if request.Size == 0 {
		request.Size = math.MaxInt64
	}
	var watcher sstore.Watcher
	defer func() {
		if watcher != nil {
			watcher.Close()
		}
	}()
	const maxBufferSize = 64 * 1024
	var buffer = make([]byte, maxBufferSize)
	for request.Size > 0 {
		var size = request.Size
		if size > maxBufferSize {
			size = maxBufferSize
		}
		n, err := reader.Read(buffer[:size])
		if err != nil {
			if err == io.EOF {
				if watcher != nil {
					watcher = store.sstore.Watcher(request.StreamId)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-watcher.Watch():
				}
				continue
			}
			return err
		}
		request.Offset += int64(n)
		request.Size -= int64(n)
		if err := callback(request.Offset, buffer[:n]); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) GetStreamStat(id int64) (*StreamStat, error) {
	begin, exist := store.sstore.Begin(id)
	if exist == false {
		return nil, fmt.Errorf("streamID no exist")
	}
	end, exist := store.sstore.End(id)
	if exist == false {
		return nil, fmt.Errorf("streamID no exist")
	}
	return &StreamStat{
		StreamID: id,
		Begin:    begin,
		End:      end,
	}, nil
}
