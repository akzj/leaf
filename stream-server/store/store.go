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
	"context"
	"fmt"
	"github.com/akzj/streamIO/pkg/sstore"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	"io"
	"math"
)

type Store struct {
	sstore *sstore.Store
}

type StreamStat struct {
	StreamID int64
	Begin    int64
	End      int64
}

func OpenStore(path string) (*Store, error) {
	sstore, err := sstore.Open(sstore.DefaultOptions(path).
		WithMaxWalSize(8 * sstore.MB).WithMaxMStreamTableSize(4 * sstore.MB))
	if err != nil {
		log.Warningf("sstore open %s failed %+v", path, err)
		return nil, err
	}
	return &Store{
		sstore: sstore,
	}, nil
}

func (store *Store) GetSStore() *sstore.Store {
	return store.sstore
}

func (store *Store) WriteRequest(request *proto.WriteStreamEntry, callback func(offset int64, err error)) {
	store.sstore.AsyncAppend(request.StreamId, request.Data, request.Offset, callback)
}

func (store *Store) ReadRequest(ctx context.Context, request *proto.ReadStreamRequest,
	callback func(offset int64, data []byte) error) error {
	var watcher *sstore.Watcher
	defer func() {
		if watcher != nil {
			watcher.Close()
		}
	}()
	reader, err := store.sstore.Reader(request.StreamId)
	if err != nil {
		unwrapErr, ok := err.(xerrors.Wrapper)
		if !request.Watch || ok == false || unwrapErr.Unwrap() != sstore.ErrNoFindStream {
			log.WithField("streamID", request.StreamId).Warn(err)
			return err
		}
		watcher = store.sstore.Watcher(request.StreamId)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-watcher.Watch():
			reader, err = store.sstore.Reader(request.StreamId)
			if err != nil {
				return err
			}
		}
	}
	end, _ := store.sstore.End(request.StreamId)
	log.WithField("end", end).Info("stream status")
	if _, err := reader.Seek(request.Offset, io.SeekStart); err != nil {
		log.WithField("Offset", request.Offset).Warn(err)
		return err
	}
	if request.Size == 0 {
		request.Size = math.MaxInt64
	}
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
				if watcher == nil {
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
