package store

import (
	"context"
	"github.com/akzj/sstore"
	"github.com/akzj/streamIO/proto"
	"io"
	"math"
)

type Store struct {
	sstore *sstore.SStore
}

func (store *Store) WriteRequest(request *proto.WriteStreamRequest, callback func(offset int64, err error)) {
	store.sstore.AsyncAppend(request.StreamId, request.Data, request.Offset, callback)
}

func (store *Store) ReadRequest(ctx context.Context, request *proto.ReadStreamRequest,
	callback func(offset int64, data []byte) error) error {
	reader, err := store.sstore.Reader(request.StreamId)
	if err != nil {
		//todo log error
		return err
	}

	if _, err := reader.Seek(request.Offset, io.SeekStart); err != nil {
		//todo log error
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
