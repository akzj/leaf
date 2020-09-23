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

package ssyncer

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/akzj/streamIO/pkg/sstore"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"io"
	"path/filepath"
	"sync/atomic"
)

/*
sync stream from master
*/
type Service struct {
	store *sstore.Store
}

func NewService(store *sstore.Store) *Service {
	return &Service{
		store: store,
	}
}

func (s *Service) SyncRequest(request *proto.SyncRequest, stream proto.SyncService_SyncRequestServer) error {
	var entryIndex int64
	err := s.store.Sync(stream.Context(), request.StreamServerId, request.Index, func(callback sstore.SyncCallback) error {
		if callback.Segment != nil {
			if err := stream.Send(&proto.SyncResponse{SegmentInfo: &proto.SegmentBegin{
				Name: filepath.Base(callback.Segment.Filename()),
				Size: callback.Segment.Size(),
			}}); err != nil {
				log.Errorf("%+v\n", err)
				return err
			}
			buffer := make([]byte, 1024*128)
			defer func() {
				_ = callback.Segment.Close()
			}()
			hash := md5.New()
			for {
				n, err := callback.Segment.Read(buffer)
				if err == io.EOF {
					if err := stream.Send(&proto.SyncResponse{
						SegmentEnd: &proto.SegmentEnd{
							Md5Sum: fmt.Sprintf("%x", hash.Sum(nil)),
						},
					}); err != nil {
						log.Error(err.Error())
						return err
					}
					log.Infof("sync segment %s done", callback.Segment.Filename())
					return nil
				}
				if err != nil {
					log.Error(err.Error())
					return err
				}
				hash.Write(buffer[:n])
				err = stream.Send(&proto.SyncResponse{SegmentData: &proto.SegmentData{
					Data: buffer[:n],
				}})
				if err != nil {
					log.Error(err.Error())
					return err
				}
			}
			return nil
		}
		var size int
		var response = proto.SyncResponse{Entries: make([]*pb.Entry, 0, 64)}
		var appendEntry = func(entry *pb.Entry) {
			if callback.Index != nil {
				atomic.AddInt64(callback.Index, 1)
			}
			if entryIndex == 0 {
				entryIndex = entry.Ver.Index
			} else {
				entryIndex++
				if entryIndex != entry.Ver.Index {
					log.Panic(entryIndex, entry)
				}
			}
			response.Entries = append(response.Entries, entry)
			size += len(entry.Data) + 32
		}
		var sendEntry = func() error {
			if len(response.Entries) != 0 {
				if err := stream.Send(&response); err != nil {
					log.Errorf(err.Error())
					return err
				}
				size = 0
				response = proto.SyncResponse{Entries: make([]*pb.Entry, 0, 64)}
			}
			return nil
		}

		var items []interface{}
		var err error
		for {
			if size == 0 {
				items, err = callback.EntryQueue.PopAll(nil)
			} else {
				items, err = callback.EntryQueue.PopAllWithoutBlock(nil)
			}
			if err != nil {
				if err == context.Canceled {
					break
				}
				return err
			}
			if len(items) == 0 {
				if err := sendEntry(); err != nil {
					return err
				}
				continue
			}
			for _, item := range items {
				var entry *pb.Entry
				switch item := item.(type) {
				case *pb.Entry:
					entry = item
				case *sstore.WriteEntry:
					entry = item.Entry
				default:
					panic(item)
				}
				appendEntry(entry)
				if size > 1024*1024 {
					if err := sendEntry(); err != nil {
						return err
					}
				}
			}
		}
		if err := sendEntry(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Warn(err)
	}
	return err
}
