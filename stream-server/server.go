package stream_server

import (
	"context"
	metaServerStore "github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	"github.com/akzj/streamIO/stream-server/store"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"os"
	"path/filepath"
	"sync"
)

type StreamServer struct {
	Options
	ctx            context.Context
	cancelFunc     context.CancelFunc
	wg             sync.WaitGroup
	ServerInfoBase *metaServerStore.ServerInfoBase
	store          *store.Store
	grpcServer     *grpc.Server
}

func New(options Options) *StreamServer {
	_ = os.MkdirAll(filepath.Dir(options.LogPath), 0777)
	file, err := os.OpenFile(options.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Panic(err.Error())
	}
	log.SetOutput(file)

	return &StreamServer{
		Options:        options,
		ServerInfoBase: nil,
		store:          nil,
	}
}

func (server *StreamServer) init() error {
	server.ctx, server.cancelFunc = context.WithCancel(context.Background())
	server.ServerInfoBase = &metaServerStore.ServerInfoBase{
		Id:     server.Options.ServerID,
		Leader: true,
		Addr:   server.Options.GRPCBindAddr,
	}
	var err error
	server.store, err = store.OpenStore(server.Options.SStorePath)
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
		log.Error(err)
		return err
	}
	if err := server.grpcServer.Serve(listener); err != nil {
		log.Error(err)
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
