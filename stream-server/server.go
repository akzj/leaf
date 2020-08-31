package stream_server

import (
	"context"
	metaServerStore "github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	"github.com/akzj/streamIO/stream-server/store"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"sync"
)

type StreamServer struct {
	Options
	ctx            context.Context
	cancelFunc     context.CancelFunc
	wg             sync.WaitGroup
	ServerInfoBase *metaServerStore.ServerInfoBase
	store          *store.Store
	log            *logrus.Logger
	grpcServer     *grpc.Server
}


func NewStreamServer(options Options) *StreamServer {
	return &StreamServer{
		Options:        options,
		ServerInfoBase: nil,
		store:          nil,
	}
}

func (server *StreamServer) init() error {
	server.log = logrus.New()
	server.ctx, server.cancelFunc = context.WithCancel(context.Background())
	server.ServerInfoBase = &metaServerStore.ServerInfoBase{
		Id:     server.Options.ServerID,
		Leader: true,
		Addr:   server.Options.GRPCBindAddr,
	}
	var err error
	server.store, err = store.OpenStore(server.Options.StorePath, server.log)
	if err != nil {
		return err
	}
	server.grpcServer = grpc.NewServer()
	proto.RegisterStreamServiceServer(server.grpcServer, server)
	return nil
}

func (server *StreamServer) startGrpcServer() error {
	listener, err := net.Listen("tcp", server.GRPCBindAddr)
	if err != nil {
		server.log.Error(err)
		return err
	}
	if err := server.grpcServer.Serve(listener); err != nil {
		server.log.Error(err)
		return err
	}
	return nil
}

func (server *StreamServer) Start() error {
	if err := server.init(); err != nil {
		return err
	}
	go server.heartbeatLoop()
	if err := server.startGrpcServer(); err != nil {
		return err
	}
	return nil
}

func (server *StreamServer) Stop(ctx context.Context) error {
	ch := make(chan interface{})
	go func() {
		server.wg.Wait()
		close(ch)
	}()
	server.cancelFunc()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
