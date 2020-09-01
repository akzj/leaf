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
			//todo log error
			return nil
		}
		if err != nil {
			log.WithError(err).Warningf("heartbeat stream server ID %d", streamServerID)
			return err
		}
		streamServerID = item.Base.Id
		if err := server.store.InsertStreamServerHeartbeatItem(item); err != nil {
			if strings.Contains(err.Error(), mmdb.ErrConflict.Error()) {
				return status.Error(codes.Aborted, err.Error())
			}
			return status.Error(codes.Internal, err.Error())
		}
		if err = stream.Send(&empty.Empty{}); err != nil {
			//todo log error
			return err
		}
	}
}

func (server *MetaServer) AddStreamServer(ctx context.Context, request *proto.AddStreamServerRequest) (*proto.AddStreamServerResponse, error) {
	if request.StreamServerInfoItem.GetBase() == nil {
		//todo log error
		return nil, status.Error(codes.InvalidArgument, "request.StreamServerInfoItem.Base nil error")
	}
	streamServerInfoItem, err := server.store.AddStreamServer(request.StreamServerInfoItem)
	if err != nil {
		//todo log error
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &proto.AddStreamServerResponse{StreamServerInfoItem: streamServerInfoItem}, nil
}

func (server *MetaServer) ListStreamServer(ctx context.Context, empty *empty.Empty) (*proto.ListStreamServerResponse, error) {
	streamServerInfoItems, err := server.store.ListStreamServer()
	if err != nil {
		//todo log error
		return nil, status.Error(codes.Internal, err.Error())
	}
	if len(streamServerInfoItems) == 0 {
		//todo log error
		return nil, status.Error(codes.NotFound, "no find streamServerInfoItems")
	}
	return &proto.ListStreamServerResponse{Items: streamServerInfoItems}, nil
}

func (server *MetaServer) GetStreamServer(ctx context.Context, request *proto.GetStreamServerRequest) (*store.StreamServerInfoItem, error) {
	item, err := server.store.GetStreamServerInfo(request.StreamServerID)
	if err != nil {
		log.Warning(err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}
	if item == nil {
		return nil, status.Error(codes.NotFound, "no find streamServerId")
	}
	return item, nil
}

func (server *MetaServer) DeleteStreamServer(ctx context.Context, request *proto.DeleteStreamServerRequest) (*empty.Empty, error) {
	err := server.store.DeleteStreamServer(request.StreamServerInfoItem)
	if err != nil {
		//todo log error
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &empty.Empty{}, nil
}

func (server *MetaServer) CreateStream(ctx context.Context, request *proto.CreateStreamRequest) (*proto.CreateStreamResponse, error) {
	streamInfoItem, create, err := server.store.CreateStream(request.Name)
	if err != nil {
		//todo log error
		return nil, status.Error(codes.Aborted, err.Error())
	}
	if create {
		return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("stream with name %s is exit", request.Name))
	}
	return &proto.CreateStreamResponse{
		Info: streamInfoItem,
	}, nil
}

func (server *MetaServer) GetOrCreateStream(ctx context.Context, request *proto.GetStreamInfoRequest) (*proto.GetStreamInfoResponse, error) {
	streamInfoItem, create, err := server.store.CreateStream(request.Name)
	if err != nil {
		//todo log error
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &proto.GetStreamInfoResponse{
		Info:   streamInfoItem,
		Create: create,
	}, nil
}

func (server *MetaServer) GetStreamInfo(ctx context.Context, request *proto.GetStreamInfoRequest) (*proto.GetStreamInfoResponse, error) {
	streamInfoItem, err := server.store.GetStream(request.Name)
	if err != nil {
		//todo log error
		return nil, status.Error(codes.Internal, err.Error())
	}
	if streamInfoItem == nil {
		//todo log error
		return nil, status.Error(codes.NotFound, "no find StreamInfo")
	}
	return &proto.GetStreamInfoResponse{Info: streamInfoItem}, nil
}

func (server *MetaServer) SetStreamReadOffset(ctx context.Context, request *proto.SetStreamReadOffsetRequest) (*empty.Empty, error) {
	if err := server.store.SetOffSet(request.SSOffsets); err != nil {
		if strings.Contains(err.Error(), mmdb.ErrConflict.Error()) {
			//todo log error
			return nil, status.Error(codes.Aborted, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &empty.Empty{}, nil
}

func (server *MetaServer) GetStreamReadOffset(ctx context.Context, request *proto.GetStreamReadOffsetRequest) (*proto.GetStreamReadOffsetResponse, error) {
	offset, err := server.store.GetOffset(request.SessionId, request.StreamId)
	if err != nil {
		//todo log error
		return nil, status.Error(codes.Internal, err.Error())
	}
	if offset == nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &proto.GetStreamReadOffsetResponse{SSOffset: offset}, nil
}
