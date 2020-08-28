package meta_server

import (
	"context"
	"github.com/akzj/mmdb"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"strings"
)

type MetaServer struct {
	store *store.Store
}

func (server *MetaServer) StreamServerHeartbeat(HeartbeatServer proto.MetaService_StreamServerHeartbeatServer) error {
	for {
		item, err := HeartbeatServer.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := server.store.InsertStreamServerHeartbeatItem(item); err != nil {
			if strings.Contains(err.Error(), mmdb.ErrConflict.Error()) {
				return status.Error(codes.Aborted, err.Error())
			}
			return status.Error(codes.Internal, err.Error())
		}
	}
}

func (server *MetaServer) AddStreamServer(ctx context.Context, request *proto.AddStreamServerRequest) (*proto.AddStreamServerResponse, error) {
	if request.StreamServerInfoItem.GetBase() == nil {
		return nil, status.Error(codes.InvalidArgument, "request.StreamServerInfoItem.Base nil error")
	}
	streamServerInfoItem, err := server.store.AddStreamServer(request.StreamServerInfoItem)
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &proto.AddStreamServerResponse{StreamServerInfoItem: streamServerInfoItem}, nil
}

func (server *MetaServer) ListStreamServer(ctx context.Context, empty *empty.Empty) (*proto.ListStreamServerResponse, error) {
	streamServerInfoItems, err := server.store.ListStreamServer()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if len(streamServerInfoItems) == 0 {
		return nil, status.Error(codes.NotFound, "no find streamServerInfoItems")
	}
	return &proto.ListStreamServerResponse{Items: streamServerInfoItems}, nil
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
	streamInfoItem, err := server.store.CreateStream(request.Name)
	if err != nil {
		if strings.HasPrefix(err.Error(), "stream name exist") {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &proto.CreateStreamResponse{
		Info: streamInfoItem,
	}, nil
}

func (server *MetaServer) GetStreamInfo(ctx context.Context, request *proto.GetStreamInfoRequest) (*proto.GetStreamInfoResponse, error) {
	streamInfoItem, err := server.store.GetStream(request.Name)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if streamInfoItem == nil {
		return nil, status.Error(codes.NotFound, "no find StreamInfo")
	}
	return &proto.GetStreamInfoResponse{Info: streamInfoItem}, nil
}

func (server *MetaServer) SetStreamReadOffset(ctx context.Context, request *proto.SetStreamReadOffsetRequest) (*empty.Empty, error) {
	if err := server.store.SetOffSet(request.SSOffsets); err != nil {
		if strings.Contains(err.Error(), mmdb.ErrConflict.Error()) {
			return nil, status.Error(codes.Aborted, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &empty.Empty{}, nil
}

func (server *MetaServer) GetStreamReadOffset(ctx context.Context, request *proto.GetStreamReadOffsetRequest) (*proto.GetStreamReadOffsetResponse, error) {
	ssOffsetItem, err := server.store.GetOffset(request.SessionId, request.StreamId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &proto.GetStreamReadOffsetResponse{Offset: ssOffsetItem.Offset}, nil
}
