package ssyncer

import (
	"crypto/md5"
	"fmt"
	"github.com/akzj/streamIO/pkg/sstore"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"io"
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
		} else if callback.Entry != nil {
			if err := stream.Send(&proto.SyncResponse{
				Entry: callback.Entry,
			}); err != nil {
				log.Errorf(err.Error())
				return err
			}
			for entry := range callback.Entries {
				if err := stream.Send(&proto.SyncResponse{
					Entry: entry,
				}); err != nil {
					log.Errorf(err.Error())
					return err
				}
			}
		}
		return nil
	})
}
