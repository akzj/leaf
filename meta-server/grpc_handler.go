package meta_server

import (
	"context"
	"fmt"
	"github.com/akzj/mmdb"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"strings"
)

func (server *MetaServer) StreamServerHeartbeat(stream proto.MetaService_StreamServerHeartbeatServer) error {
	var streamServerID int64
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			log.Error(err)
			return nil
		}
		if err != nil {
			log.WithError(err).Warningf("heartbeat stream server ID %d", streamServerID)
			return err
		}
		streamServerID = item.Base.Id
		if err := server.store.InsertStreamServerHeartbeatItem(item); err != nil {
			log.Error(err)
			if strings.Contains(err.Error(), mmdb.ErrConflict.Error()) {
				return status.Error(codes.Aborted, err.Error())
			}
			return status.Error(codes.Internal, err.Error())
		}
		if err = stream.Send(&empty.Empty{}); err != nil {
			log.Error(err)
			return err
		}
		log.WithField("id", item.Base.Id).WithField("addr", item.Base.Addr).Info("heartbeat")
	}
}

func (server *MetaServer) AddStreamServer(_ context.Context, request *proto.AddStreamServerRequest) (*proto.AddStreamServerResponse, error) {
	if request.StreamServerInfoItem.GetBase() == nil {
		log.Error("request.StreamServerInfoItem nil")
		return nil, status.Error(codes.InvalidArgument, "request.StreamServerInfoItem.Base nil error")
	}
	streamServerInfoItem, err := server.store.AddStreamServer(request.StreamServerInfoItem)
	if err != nil {
		log.Error(err)
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &proto.AddStreamServerResponse{StreamServerInfoItem: streamServerInfoItem}, nil
}

func (server *MetaServer) ListStreamServer(_ context.Context, _ *empty.Empty) (*proto.ListStreamServerResponse, error) {
	streamServerInfoItems, err := server.store.ListStreamServer()
	if err != nil {
		log.Error(err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	if len(streamServerInfoItems) == 0 {
		return nil, status.Error(codes.NotFound, "no find streamServerInfoItems")
	}
	return &proto.ListStreamServerResponse{Items: streamServerInfoItems}, nil
}

func (server *MetaServer) GetStreamServer(_ context.Context, request *proto.GetStreamServerRequest) (*store.StreamServerInfoItem, error) {
	item, err := server.store.GetStreamServerInfo(request.StreamServerID)
	if err != nil {
		log.Warning(err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}
	if item == nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("no find streamServerId %d", request.StreamServerID))
	}
	return item, nil
}

func (server *MetaServer) DeleteStreamServer(_ context.Context, request *proto.DeleteStreamServerRequest) (*empty.Empty, error) {
	err := server.store.DeleteStreamServer(request.StreamServerInfoItem)
	if err != nil {
		log.Error(err)
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &empty.Empty{}, nil
}

func (server *MetaServer) CreateStream(_ context.Context, request *proto.CreateStreamRequest) (*proto.CreateStreamResponse, error) {
	streamInfoItem, create, err := server.store.CreateStream(request.Name)
	if err != nil {
		log.Error(err)
		return nil, status.Error(codes.Aborted, err.Error())
	}
	if create {
		return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("stream with name %s is exit", request.Name))
	}
	return &proto.CreateStreamResponse{
		Info: streamInfoItem,
	}, nil
}

func (server *MetaServer) GetOrCreateStream(_ context.Context, request *proto.GetStreamInfoRequest) (*proto.GetStreamInfoResponse, error) {
	streamInfoItem, create, err := server.store.CreateStream(request.Name)
	if err != nil {
		log.Error(err)
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &proto.GetStreamInfoResponse{
		Info:   streamInfoItem,
		Create: create,
	}, nil
}

func (server *MetaServer) GetStreamInfo(_ context.Context, request *proto.GetStreamInfoRequest) (*proto.GetStreamInfoResponse, error) {
	streamInfoItem, err := server.store.GetStream(request.Name)
	if err != nil {
		log.Error(err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	if streamInfoItem == nil {
		return nil, status.Error(codes.NotFound, "no find StreamInfo")
	}
	return &proto.GetStreamInfoResponse{Info: streamInfoItem}, nil
}

func (server *MetaServer) SetStreamReadOffset(_ context.Context, request *proto.SetStreamReadOffsetRequest) (*empty.Empty, error) {
	if err := server.store.SetOffSet(request.SSOffsets); err != nil {
		if strings.Contains(err.Error(), mmdb.ErrConflict.Error()) {
			log.Error(err)
			return nil, status.Error(codes.Aborted, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &empty.Empty{}, nil
}

func (server *MetaServer) GetStreamReadOffset(_ context.Context, request *proto.GetStreamReadOffsetRequest) (*proto.GetStreamReadOffsetResponse, error) {
	offset, err := server.store.GetOffset(request.SessionId, request.StreamId)
	if err != nil {
		log.Error(err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	if offset == nil {
		return nil, status.Error(codes.NotFound,
			fmt.Sprintf("no find offset for streamID %d sessionID %d",
				request.StreamId, request.SessionId))
	}
	return &proto.GetStreamReadOffsetResponse{SSOffset: offset}, nil
}
