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

func (s *Service) SyncRequest(request *proto.SyncRequest, stream proto.SyncService_SyncRequestServer) error {
	if request.SyncSegmentRequest != nil {
		return s.handleSyncSegmentRequest(request.SyncSegmentRequest, stream)
	}
	var err error
	s.store.Sync(request.StreamServerId, request.Index, func(callback sstore.SyncCallback) {
		if err != nil {
			err = callback.Err
			log.Errorf(err.Error())
			return
		}
		if callback.Segment != nil {
			err = s.syncSegment(0, callback.Segment, stream)
		}
	})
	return nil
}

func (s *Service) syncSegment(offset int64, segment *sstore.SegmentReader, stream proto.SyncService_SyncRequestServer) error {
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
	return s.syncSegment(request.Offset, segment, stream)
}
