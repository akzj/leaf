package stream_server

import (
	"context"
	metaServerStore "github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/stream-server/store"
	"sync"
)

type StreamServer struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	Options
	ServerInfoBase *metaServerStore.ServerInfoBase
	store          *store.Store
}

func NewStreamServer(options Options) *StreamServer {
	return &StreamServer{
		Options:        options,
		ServerInfoBase: nil,
		store:          nil,
	}
}

func (server *StreamServer) Start() {
	go server.heartbeatLoop()
}

func (server *StreamServer) Stop(ctx context.Context) error{
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
