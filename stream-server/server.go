// Copyright 2020-2026 The streamIO Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stream_server

import (
	"context"
	"github.com/akzj/streamIO/proto"
	"github.com/akzj/streamIO/stream-server/ssyncer"
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
	ServerInfoBase    *proto.ServerInfoBase
	store             *store.Store
	streamGrpcServer  *grpc.Server
	ssyncerGrpcServer *grpc.Server
	GrpcServer        *grpc.Server
	metaServiceClient proto.MetaServiceClient

	syncService *ssyncer.Service

	syncClientLocker sync.Mutex
	syncClient       *ssyncer.Client

	count int64
}

func New(options Options) *StreamServer {
	log.SetFormatter(&log.JSONFormatter{
		PrettyPrint: true,
	})
	log.WithField("options", options).Info("new stream")
	_ = os.MkdirAll(filepath.Dir(options.LogPath), 0777)
	/*file, err := os.OpenFile(options.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Panic(err.Error())
	}*/
	//log.SetOutput(file)
	log.SetLevel(options.LogLevel)
	log.SetFormatter(&log.TextFormatter{DisableQuote: true})
	log.SetReportCaller(true)
	ctx, cancel := context.WithCancel(context.Background())
	return &StreamServer{
		Options: options,
		ctx:     ctx,
		cancel:  cancel,
		wg:      sync.WaitGroup{},
		ServerInfoBase: &proto.ServerInfoBase{
			Id:     options.ServerID,
			Leader: true,
			Addr:   net.JoinHostPort(options.Host, strconv.Itoa(options.StreamPort)),
		},
		store:             nil,
		streamGrpcServer:  nil,
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
			&proto.AddStreamServerRequest{StreamServerInfoItem: &proto.StreamServerInfoItem{
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
	server.syncService = ssyncer.NewService(server.store.GetSStore())
	server.streamGrpcServer = grpc.NewServer()
	server.ssyncerGrpcServer = grpc.NewServer()
	proto.RegisterStreamServiceServer(server.streamGrpcServer, server)
	proto.RegisterSyncServiceServer(server.ssyncerGrpcServer, server.syncService)
	return nil
}

func (server *StreamServer) startStreamServer() error {
	listener, err := net.Listen("tcp",
		net.JoinHostPort(server.Host, strconv.Itoa(server.StreamPort)))
	if err != nil {
		log.Error(err)
		return err
	}
	go server.printMsgCount()
	if err := server.streamGrpcServer.Serve(listener); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (server *StreamServer) startSSyncerServer() error {
	listener, err := net.Listen("tcp",
		net.JoinHostPort(server.Host, strconv.Itoa(server.SyncPort)))
	if err != nil {
		log.Error(err)
		return err
	}
	if err := server.ssyncerGrpcServer.Serve(listener); err != nil {
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
	go func() {
		if err := server.startSSyncerServer(); err != nil {
			log.Fatalf(err.Error())
		}
	}()
	if server.Options.SyncFrom != "" {
		if _, err := server.StartSyncFrom(server.ctx, &proto.SyncFromRequest{Addr: server.SyncFrom}); err != nil {
			panic(err)
		}
	}
	if err := server.startStreamServer(); err != nil {
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

func (server *StreamServer) syncStreamFailed(streamServerAddr string, err error) {
	//todo notify to cluster manager for dispatching new stream master server to sync
}
