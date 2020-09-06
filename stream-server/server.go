package stream_server

import (
	"context"
	MSStore "github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	"github.com/akzj/streamIO/stream-server/store"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type StreamServer struct {
	Options
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	ServerInfoBase    *MSStore.ServerInfoBase
	store             *store.Store
	grpcServer        *grpc.Server
	metaServiceClient proto.MetaServiceClient
}

func New(options Options) *StreamServer {
	_ = os.MkdirAll(filepath.Dir(options.LogPath), 0777)
	file, err := os.OpenFile(options.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Panic(err.Error())
	}
	log.SetOutput(file)
	log.SetLevel(options.LogLevel)
	log.SetFormatter(&log.TextFormatter{DisableQuote: true})
	ctx, cancel := context.WithCancel(context.Background())
	return &StreamServer{
		Options: options,
		ctx:     ctx,
		cancel:  cancel,
		wg:      sync.WaitGroup{},
		ServerInfoBase: &MSStore.ServerInfoBase{
			Id:     options.ServerID,
			Leader: true,
			Addr:   net.JoinHostPort(options.Host, strconv.Itoa(options.GRPCPort)),
		},
		store:             nil,
		grpcServer:        nil,
		metaServiceClient: nil,
	}
}

func (server *StreamServer) init() error {
	ctx, cancel := context.WithTimeout(server.ctx, server.DialMetaServerTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, server.Options.MetaServerAddr, grpc.WithInsecure())
	if err != nil {
		return errors.WithStack(err)
	}
	client := proto.NewMetaServiceClient(conn)
	server.metaServiceClient = client

	if server.AutoAddServer {
		addStreamServerResponse, err := server.metaServiceClient.AddStreamServer(server.ctx,
			&proto.AddStreamServerRequest{StreamServerInfoItem: &MSStore.StreamServerInfoItem{
				Base: server.ServerInfoBase}})
		if err != nil {
			if status.Code(err) != codes.AlreadyExists {
				return errors.WithStack(err)
			}
		}
		if server.ServerInfoBase.Id == 0 {
			server.ServerInfoBase = addStreamServerResponse.StreamServerInfoItem.Base
		}
	}

	server.store, err = store.OpenStore(server.Options.SStorePath)
	if err != nil {
		return err
	}
	server.grpcServer = grpc.NewServer()
	proto.RegisterStreamServiceServer(server.grpcServer, server)
	return nil
}

func (server *StreamServer) startGrpcServer() error {
	listener, err := net.Listen("tcp",
		net.JoinHostPort(server.Host, strconv.Itoa(server.GRPCPort)))
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
	server.cancel()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
