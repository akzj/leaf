package client

import (
	"context"
	block_queue "github.com/akzj/block-queue"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
)

const minWriteSize = 4 * 1024

type StreamReader interface {
	io.ReadSeeker
	io.Closer
	Offset() int64
}

type StreamWriter interface {
	io.WriteCloser
	Flush() error
	WriteWithCb(data []byte, callback func(err error))
}

type StreamSession interface {
	NewReader() (StreamReader, error)
	NewWriter() (StreamWriter, error)
	SetReadOffset(offset int64) error
	GetReadOffset() (offset int64, err error)
}

type Client interface {
	Close() error
	//streamServer
	AddStreamServer(ctx context.Context, StreamServerID int64, addr string) error

	//stream
	CreateStream(ctx context.Context, name string) (streamID int64, err error)
	GetStreamID(ctx context.Context, name string) (streamID int64, err error)
	GetOrCreateStream(ctx context.Context, name string) (streamID int64, err error)

	//session
	NewStreamSession(ctx context.Context, sessionID int64, name string) (StreamSession, error)
	NewStreamWriter(ctx context.Context, streamID int64, streamServerID int64) (StreamWriter, error)

	//MQTT
	GetOrCreateMQTTSession(ctx context.Context, clientIdentifier string) (*store.MQTTSessionItem, bool, error)
	UpdateMQTTClientSession(ctx context.Context, clientIdentifier string, Unsubscribe []string, subscribe map[string]int32) error
	DeleteMQTTClientSession(ctx context.Context, clientIdentifier string) (*store.MQTTSessionItem, error)
}

type client struct {
	streamServiceClientLocker sync.Mutex
	metaServerClient          proto.MetaServiceClient
	streamServiceClient       map[int64]proto.StreamServiceClient

	setReadOffsetRequestQueue *block_queue.Queue

	streamRequestWritersLocker sync.Mutex
	streamRequestWriters       map[int64]*streamRequestWriter
}

func (c *client) NewStreamWriter(ctx context.Context, streamID int64, streamServerID int64) (StreamWriter, error) {
	streamServiceClient, err := c.getStreamClient(ctx, streamServerID)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	queue := c.startStreamRequestWriter(streamServerID, streamServiceClient)
	return &streamWriter{
		streamID: streamID,
		queue:    queue,
	}, nil
}

type setReadOffsetRequest struct {
	item  *store.SSOffsetItem
	close bool
	cb    func(err error)
}

func NewMetaServiceClient(ctx context.Context, Addr string) (proto.MetaServiceClient, error) {
	conn, err := grpc.DialContext(ctx, Addr, grpc.WithInsecure())
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return proto.NewMetaServiceClient(conn), nil
}

func NewClient(sc proto.MetaServiceClient) Client {
	var c = &client{
		streamServiceClientLocker: sync.Mutex{},
		metaServerClient:          sc,
		streamServiceClient:       make(map[int64]proto.StreamServiceClient),
		setReadOffsetRequestQueue: block_queue.NewQueue(10240),
	}
	go c.processSetReadOffsetRequestLoop()
	return c
}

func (c *client) putSetReadOffsetRequest(request setReadOffsetRequest) {
	c.setReadOffsetRequestQueue.Push(request)
}

func (c *client) startStreamRequestWriter(streamServerID int64,
	streamServiceClient proto.StreamServiceClient) *block_queue.Queue {
	c.streamRequestWritersLocker.Lock()
	defer c.streamRequestWritersLocker.Unlock()

	writer, ok := c.streamRequestWriters[streamServerID]
	if ok == false {
		writer = newWriteStreamRequest(context.Background(), streamServiceClient)
		c.streamRequestWriters[streamServerID] = writer
		go writer.writeLoop()
	}
	return writer.queue
}

func (c *client) processSetReadOffsetRequestLoop() {
	var buf []interface{}
	var isClosed = false
	for isClosed == false {
		var setReadOffsetRequests []setReadOffsetRequest
		var setStreamReadOffsetRequest = &proto.SetStreamReadOffsetRequest{}
		items := c.setReadOffsetRequestQueue.PopAll(buf)
		for _, item := range items {
			request := item.(setReadOffsetRequest)
			if request.close {
				isClosed = true
				continue
			}
			setReadOffsetRequests = append(setReadOffsetRequests, request)
			setStreamReadOffsetRequest.SSOffsets = append(setStreamReadOffsetRequest.SSOffsets, request.item)
		}
		if setStreamReadOffsetRequest.SSOffsets != nil {
			_, err := c.metaServerClient.SetStreamReadOffset(context.Background(), setStreamReadOffsetRequest)
			for _, request := range setReadOffsetRequests {
				request.cb(err)
			}
		}
	}
}

func (c *client) AddStreamServer(ctx context.Context, StreamServerID int64, addr string) error {
	_, err := c.metaServerClient.AddStreamServer(ctx,
		&proto.AddStreamServerRequest{StreamServerInfoItem:
		&store.StreamServerInfoItem{Base:
		&store.ServerInfoBase{
			Id:     StreamServerID,
			Leader: true,
			Addr:   addr,
		}}})
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (c *client) getMetaServiceClient() (proto.MetaServiceClient, error) {
	return c.metaServerClient, nil
}

func (c *client) CreateStream(ctx context.Context, name string) (streamID int64, err error) {
	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		log.Error(err)
		return 0, err
	}

	response, err := metaServiceClient.CreateStream(ctx, &proto.CreateStreamRequest{Name: name})
	if err != nil {
		log.Error(err)
		return 0, err
	}
	return response.Info.StreamId, nil
}

func (c *client) GetStreamID(ctx context.Context, name string) (streamID int64, err error) {
	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		return 0, err
	}

	response, err := metaServiceClient.CreateStream(ctx, &proto.CreateStreamRequest{Name: name})
	if err != nil {
		return 0, err
	}
	return response.Info.StreamId, nil
}

func (c *client) GetOrCreateMQTTSession(ctx context.Context, clientIdentifier string) (*store.MQTTSessionItem, bool, error) {
	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		log.Error(err)
		return nil, false, err
	}
	req := &proto.GetOrCreateMQTTClientSessionRequest{ClientIdentifier: clientIdentifier}
	response, err := metaServiceClient.GetOrCreateMQTTClientSession(ctx, req)
	if err != nil {
		return nil, false, err
	}
	return response.SessionItem, response.Create, nil
}

func (c *client) UpdateMQTTClientSession(ctx context.Context,
	clientIdentifier string,
	Unsubscribe []string,
	subscribe map[string]int32) error {

	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = metaServiceClient.UpdateMQTTClientSession(ctx, &proto.UpdateMQTTClientSessionRequest{
		ClientIdentifier: clientIdentifier,
		Subscribe:        subscribe,
		UnSubscribe:      Unsubscribe,
	})
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (c *client) DeleteMQTTClientSession(ctx context.Context, clientIdentifier string) (*store.MQTTSessionItem, error) {
	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	response, err := metaServiceClient.DeleteMQTTClientSession(ctx,
		&proto.DeleteMQTTClientSessionRequest{ClientIdentifier: clientIdentifier})
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	return response.SessionItem, nil
}

func (c *client) GetOrCreateStream(ctx context.Context, name string) (streamID int64, err error) {
	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		return 0, err
	}

	response, err := metaServiceClient.GetOrCreateStream(ctx, &proto.GetStreamInfoRequest{Name: name})
	if err != nil {
		return 0, err
	}
	return response.Info.StreamId, nil
}

func (c *client) NewStreamSession(ctx context.Context, sessionID int64, name string) (StreamSession, error) {
	streamID, err := c.GetOrCreateStream(ctx, name)
	if err != nil {
		return nil, err
	}
	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		return nil, err
	}
	return &session{
		name:             name,
		readBuffSize:     1024,
		ctx:              ctx,
		sessionID:        sessionID,
		streamID:         streamID,
		client:           c,
		metaServerClient: metaServiceClient,
	}, nil
}

func (c *client) getStreamClient(ctx context.Context, streamServerID int64) (proto.StreamServiceClient, error) {
	c.streamServiceClientLocker.Lock()
	defer c.streamServiceClientLocker.Unlock()
	if client, ok := c.streamServiceClient[streamServerID]; ok {
		return client, nil
	}
	response, err := c.metaServerClient.GetStreamServer(context.Background(),
		&proto.GetStreamServerRequest{StreamServerID: streamServerID})
	if err != nil {
		return nil, err
	}
	conn, err := grpc.DialContext(ctx, response.Base.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := proto.NewStreamServiceClient(conn)
	c.streamServiceClient[streamServerID] = client
	return client, nil
}

func (c *client) Close() error {
	return nil
}

type session struct {
	name             string
	readBuffSize     int64
	ctx              context.Context
	sessionID        int64
	streamID         int64
	client           *client
	metaServerClient proto.MetaServiceClient
}

func (s *session) NewReader() (StreamReader, error) {
	response, err := s.metaServerClient.GetStreamInfo(s.ctx, &proto.GetStreamInfoRequest{Name: s.name})
	if err != nil {
		return nil, err
	}
	streamID := response.Info.StreamId
	offset, err := s.GetReadOffset()
	if err != nil {
		errStatus := status.Convert(err)
		if errStatus.Code() != codes.NotFound {
			return nil, err
		}
	}
	streamClient, err := s.client.getStreamClient(s.ctx, response.Info.StreamServerId)
	if err != nil {
		return nil, err
	}
	return &streamReader{
		readBuffSize:    s.readBuffSize,
		offset:          offset,
		streamID:        streamID,
		rpcStreamReader: nil,
		reader:          nil,
		client:          streamClient,
		ctx:             s.ctx,
		session:         s,
	}, nil
}

func (s *session) NewWriter() (StreamWriter, error) {
	response, err := s.metaServerClient.GetStreamInfo(s.ctx, &proto.GetStreamInfoRequest{Name: s.name})
	if err != nil {
		return nil, err
	}
	streamServiceClient, err := s.client.getStreamClient(s.ctx, response.Info.StreamServerId)
	if err != nil {
		return nil, err
	}
	queue := s.client.startStreamRequestWriter(response.Info.StreamServerId,
		streamServiceClient)
	return &streamWriter{
		streamID: response.Info.StreamId,
		queue:    queue,
	}, nil
}

var chanPool = sync.Pool{New: func() interface{} { return make(chan interface{}, 1) }}

func (s *session) SetReadOffset(offset int64) error {
	var err error
	ch := chanPool.Get().(chan interface{})
	s.client.putSetReadOffsetRequest(setReadOffsetRequest{
		item: &store.SSOffsetItem{Offset: offset, SessionId: s.sessionID, StreamId: s.streamID},
		cb: func(e error) {
			err = e
			ch <- struct{}{}
		},
	})
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-ch:
		chanPool.Put(ch)
		return err
	}
}

func (s *session) GetReadOffset() (offset int64, err error) {
	response, err := s.metaServerClient.GetStreamReadOffset(s.ctx,
		&proto.GetStreamReadOffsetRequest{StreamId: s.streamID, SessionId: s.sessionID})
	if err != nil {
		return 0, err
	}
	return response.SSOffset.Offset, nil
}
