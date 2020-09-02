package meta_server

import (
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"os"
	"path/filepath"
	"strconv"
)

type MetaServer struct {
	Options
	store *store.Store
}

func initLog(options Options) {
	_ = os.MkdirAll(filepath.Dir(options.LogFile), 0777)
	file, err := os.OpenFile(options.LogFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err.Error())
	}
	log.SetOutput(file)
	log.SetReportCaller(true)
	log.SetLevel(options.LogLevel)
}

func NewMetaServer(options Options) *MetaServer {
	initLog(options)
	return &MetaServer{
		Options: options,
		store:   store.OpenStore(options.MMdbOptions),
	}
}

func (server *MetaServer) Start() error {
	listener, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(server.GRPCBind))
	if err != nil {
		log.Error(err)
		return err
	}
	s := grpc.NewServer()
	proto.RegisterMetaServiceServer(s, server)
	log.WithField("pid", os.Getpid()).Infof("meta-server bind %d", server.GRPCBind)
	if err := s.Serve(listener); err != nil {
		log.Error(err)
		return err
	}
	return nil
}
