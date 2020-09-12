package ssyncer

import (
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
	if request.SyncSegmentRequest != nil {
		return s.handleSyncSegmentRequest(request.SyncSegmentRequest, stream)
	}
	return s.store.Sync(stream.Context(), request.StreamServerId, request.Index, func(callback sstore.SyncCallback) error {
		if callback.Segment != nil {
			if err := stream.Send(&proto.SyncResponse{SegmentInfo: &proto.SegmentInfo{
				Name: callback.Segment.Filename(),
				Size: callback.Segment.Size(),
			}}); err != nil {
				log.Errorf("%+v\n", err)
				return err
			}
			return s.syncSegmentData(0, callback.Segment, stream)
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

func (s *Service) syncSegmentData(offset int64, segment *sstore.SegmentReader, stream proto.SyncService_SyncRequestServer) error {
	defer func() {
		if err := segment.Close(); err != nil {
			log.Errorf(err.Error())
		}
	}()
	buffer := make([]byte, 1024*128)
	for {
		n, err := segment.Read(buffer)
		if err == io.EOF {
			if err := stream.Send(&proto.SyncResponse{
				SegmentInfo: nil,
				SegmentData: nil,
				SegmentEnd:  &proto.SegmentEnd{},
			}); err != nil {
				log.Error(err.Error())
				return err
			}
			log.Info("start SyncSegment done")
			return nil
		}
		if err != nil {
			log.Error(err.Error())
			return err
		}
		err = stream.Send(&proto.SyncResponse{SegmentData: &proto.SegmentData{
			Offset: offset,
			Data:   buffer[:n],
		}})
		if err != nil {
			log.Error(err.Error())
			return err
		}
		offset += int64(n)
	}
}

func (s *Service) handleSyncSegmentRequest(request *proto.SyncSegmentRequest,
	stream proto.SyncService_SyncRequestServer) error {

	logEntry := log.WithField("segment", request.Name).
		WithField("offset", request.Offset)
	logEntry.Info("start SyncSegment")
	segment, err := s.store.OpenSegmentReader(request.Name)
	if err != nil {
		logEntry.Error(err)
		return err
	}
	if _, err := segment.Seek(request.Offset, io.SeekStart); err != nil {
		_ = segment.Close()
		logEntry.Error(err)
		return err
	}
	return s.syncSegmentData(request.Offset, segment, stream)
}
