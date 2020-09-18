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
	"sync/atomic"
)

/*
ssyncer stream from master
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
				Name: callback.Segment.Filename(),
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
		} else {
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
			var responseEntry = func() error {
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

			if callback.Entry != nil {
				appendEntry(callback.Entry)
				callback.Entry = nil
			}
			if callback.EntryQueue != nil {
				for {
					items, err := callback.EntryQueue.PopAll(nil)
					if err != nil {
						if err == context.Canceled {
							break
						}
						return err
					}
					for _, item := range items {
						var entry *pb.Entry
						switch item := item.(type) {
						case *pb.Entry:
							entry = item
						case *sstore.WriteEntryRequest:
							entry = item.Entry
						default:
							panic(item)
						}
						appendEntry(entry)
						if size > 1024*1024 {
							if err := responseEntry(); err != nil {
								return err
							}
						}
					}
					if err := responseEntry(); err != nil {
						return err
					}
				}
				if err := responseEntry(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Warn(err)
	}
	return err
}
