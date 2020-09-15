package ssyncer

import (
	"crypto/md5"
	"fmt"
	"github.com/akzj/streamIO/pkg/sstore"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"io"
	"time"
)

/*
ssyncer stream from master
*/
type Service struct {
	store *sstore.SStore
}

func NewService(store *sstore.SStore) *Service {
	return &Service{
		store: store,
	}
}

func (s *Service) SyncRequest(request *proto.SyncRequest, stream proto.SyncService_SyncRequestServer) error {
	return s.store.Sync(stream.Context(), request.StreamServerId, request.Index, func(callback sstore.SyncCallback) error {
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
			if callback.Entry != nil {
				fmt.Println("callback entry", callback.Entry.Ver)
				if err := stream.Send(&proto.SyncResponse{
					Entries: []*pb.Entry{callback.Entry},
				}); err != nil {
					log.Errorf(err.Error())
					return err
				}
				callback.Entry = nil
			}
			if callback.Entries != nil {
				tick := time.NewTicker(time.Millisecond * 10)
				defer tick.Stop()
				var end = false
				for !end {
					var size int
					var response = proto.SyncResponse{Entries: make([]*pb.Entry, 0, 64)}
					select {
					case entry := <-callback.Entries:
						if entry == nil {
							return nil //
						}
						response.Entries = append(response.Entries, entry)
						size += len(entry.Data) + 32
					}
					begin := time.Now()
					for {
						select {
						case entry := <-callback.Entries:
							if entry == nil {
								end = true
								goto responseEntry
							}
							response.Entries = append(response.Entries, entry)
							size += len(entry.Data) + 32
							if size > 1024*1024 {
								goto responseEntry
							}
						case ts := <-tick.C:
							if begin.Sub(ts) > time.Millisecond*10 {
								goto responseEntry
							}
						}
					}

				responseEntry:
					if len(response.Entries) != 0 {
						fmt.Println("entries", len(response.Entries))
						if err := stream.Send(&response); err != nil {
							log.Errorf(err.Error())
							return err
						}
					}
				}
			}
		}
		return nil
	})
}
