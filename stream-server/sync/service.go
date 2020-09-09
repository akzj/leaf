package sync

import (
	"github.com/akzj/streamIO/pkg/sstore"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"io"
)

/*
sync stream from master
*/
type Service struct {
	store *sstore.SStore
}

func (s *Service) SyncRequest(request *proto.SyncRequest, server proto.SyncService_SyncRequestServer) error {
	if request.SyncSegmentRequest != nil {
		return s.handleSyncSegmentRequest(request.SyncSegmentRequest, server)
	}
	s.store.Sync(request.Index)
}

func (s *Service) handleSyncSegmentRequest(request *proto.SyncSegmentRequest, stream proto.SyncService_SyncRequestServer) error {
	logEntry := log.WithField("segment", request.Name).
		WithField("offset", request.Offset)
	logEntry.Info("start SyncSegment")
	segment, err := s.store.OpenSegment(request.Name)
	if err != nil {
		logEntry.Error(err)
		return err
	}
	defer func() {
		if err := segment.Close(); err != nil {
			log.Errorf(err.Error())
		}
	}()
	if _, err := segment.Seek(request.Offset, io.SeekStart); err != nil {
		logEntry.Error(err)
		return err
	}
	buffer := make([]byte, 1024*128)
	var offset = request.Offset
	for {
		n, err := segment.Read(buffer)
		if err == io.EOF {
			if err := stream.Send(&proto.SyncResponse{
				SegmentInfo: nil,
				SegmentData: nil,
				SegmentEnd:  &proto.SegmentEnd{},
			}); err != nil {
				logEntry.Error(err.Error())
				return err
			}
			logEntry.Info("start SyncSegment done")
			return nil
		}
		if err != nil {
			logEntry.Error(err.Error())
			return err
		}
		err = stream.Send(&proto.SyncResponse{SegmentData: &proto.SegmentData{
			Offset: offset,
			Data:   buffer[:n],
		}})
		if err != nil {
			logEntry.Error(err.Error())
			return err
		}
		offset += int64(n)
	}
}
