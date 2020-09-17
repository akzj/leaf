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
package client

import (
	"bytes"
	"context"
	block_queue "github.com/akzj/streamIO/pkg/block-queue"
	"github.com/akzj/streamIO/proto"
	"github.com/pkg/errors"
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
	WriteWithCb(data []byte, callback func(err error))
	StreamID() int64
}

type StreamSession interface {
	NewReader() (StreamReader, error)
	NewWriter() (StreamWriter, error)
	SetReadOffset(offset int64) error
	SetReadOffsetWithCb(offset int64, f func(err error))
	GetReadOffset() (offset int64, err error)
}

type Client interface {
	Close() error
	//streamServer
	AddStreamServer(ctx context.Context, StreamServerID int64, addr string) error

	//stream
	GetOrCreateStreamInfoItem(ctx context.Context, name string) (item *proto.StreamInfoItem, err error)
	CreateStreamInfoItem(ctx context.Context, name string) (item *proto.StreamInfoItem, err error)
	GetStreamInfoItem(ctx context.Context, name string) (item *proto.StreamInfoItem, err error)
	GetStreamStat(ctx context.Context, item *proto.StreamInfoItem) (begin int64, end int64, err error)
	SetStreamReadOffset(ctx context.Context, sessionID int64, offset int64, item *proto.StreamInfoItem) error

	CreateSessionAndReader(ctx context.Context,
		sessionID int64, streamInfo *proto.StreamInfoItem) (StreamSession, StreamReader, error)

	//session
	NewStreamSession(ctx context.Context, sessionID int64, streamInfo *proto.StreamInfoItem) (StreamSession, error)

	//MQTT
	GetOrCreateMQTTSession(ctx context.Context, clientIdentifier string) (*proto.MQTTSessionItem, bool, error)
	UpdateMQTTClientSession(ctx context.Context, clientIdentifier string, Unsubscribe []string, subscribe map[string]int32) error
	DeleteMQTTClientSession(ctx context.Context, clientIdentifier string) (*proto.MQTTSessionItem, error)
}

type client struct {
	streamServiceClientLocker sync.Mutex
	metaServerClient          proto.MetaServiceClient
	streamServiceClient       map[int64]proto.StreamServiceClient

	setReadOffsetRequestQueue *block_queue.Queue

	streamRequestWritersLocker sync.Mutex
	streamRequestWriters       map[int64]*streamRequestWriter
}

func (c *client) CreateSessionAndReader(ctx context.Context,
	sessionID int64, streamInfo *proto.StreamInfoItem) (StreamSession, StreamReader, error) {
	session, err := c.NewStreamSession(ctx, sessionID, streamInfo)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	offset, err := session.GetReadOffset()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	reader, err := session.NewReader()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if offset != 0 {
		if _, err := reader.Seek(offset, io.SeekStart); err != nil {
			_ = reader.Close()
			return nil, nil, errors.WithStack(err)
		}
	}
	return session, reader, nil
}

func (c *client) NewStreamWriter(ctx context.Context, item proto.StreamInfoItem) (StreamWriter, error) {
	streamServiceClient, err := c.getStreamClient(ctx, item.StreamServerId)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	writeStreamRequests := c.startStreamRequestWriter(item.StreamServerId, streamServiceClient)
	return &streamWriter{
		locker: sync.Mutex{},
		buffer: bytes.Buffer{},
		queue:  writeStreamRequests,
	}, nil
}

type setReadOffsetRequest struct {
	item  *proto.SSOffsetItem
	close bool
	cb    func(err error)
}

func NewMetaServiceClient(ctx context.Context, Addr string) (proto.MetaServiceClient, error) {
	conn, err := grpc.DialContext(ctx, Addr, grpc.WithInsecure())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return proto.NewMetaServiceClient(conn), nil
}

func NewClient(sc proto.MetaServiceClient) Client {
	var c = &client{
		streamServiceClientLocker:  sync.Mutex{},
		metaServerClient:           sc,
		streamServiceClient:        make(map[int64]proto.StreamServiceClient),
		setReadOffsetRequestQueue:  block_queue.NewQueue(1024),
		streamRequestWritersLocker: sync.Mutex{},
		streamRequestWriters:       map[int64]*streamRequestWriter{},
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
	return writer.requestQueue
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
		&proto.StreamServerInfoItem{Base:
		&proto.ServerInfoBase{
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

func (c *client) CreateStreamInfoItem(ctx context.Context, name string) (streamID *proto.StreamInfoItem, err error) {
	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	response, err := metaServiceClient.CreateStream(ctx, &proto.CreateStreamRequest{Name: name})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return response.Info, nil
}

func (c *client) GetStreamInfoItem(ctx context.Context, name string) (info *proto.StreamInfoItem, err error) {
	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	response, err := metaServiceClient.CreateStream(ctx, &proto.CreateStreamRequest{Name: name})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return response.Info, nil
}

func (c *client) GetOrCreateMQTTSession(ctx context.Context, clientIdentifier string) (*proto.MQTTSessionItem, bool, error) {
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
		return errors.WithStack(err)
	}
	_, err = metaServiceClient.UpdateMQTTClientSession(ctx, &proto.UpdateMQTTClientSessionRequest{
		ClientIdentifier: clientIdentifier,
		Subscribe:        subscribe,
		UnSubscribe:      Unsubscribe,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *client) DeleteMQTTClientSession(ctx context.Context, clientIdentifier string) (*proto.MQTTSessionItem, error) {
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

func (c *client) GetOrCreateStreamInfoItem(ctx context.Context, name string) (streamID *proto.StreamInfoItem, err error) {
	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	response, err := metaServiceClient.GetOrCreateStream(ctx, &proto.GetStreamInfoRequest{Name: name})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return response.Info, nil
}

func (c *client) SetStreamReadOffset(ctx context.Context, sessionID int64, offset int64, item *proto.StreamInfoItem) error {
	client, err := c.getMetaServiceClient()
	if err != nil {
		return err
	}
	_, err = client.SetStreamReadOffset(ctx, &proto.SetStreamReadOffsetRequest{SSOffsets: []*proto.SSOffsetItem{{
		SessionId: sessionID,
		StreamId:  item.StreamId,
		Offset:    offset,
	}}})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *client) GetStreamStat(ctx context.Context, item *proto.StreamInfoItem) (int64, int64, error) {
	streamClient, err := c.getStreamClient(ctx, item.StreamServerId)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	stat, err := streamClient.GetStreamStat(ctx, &proto.GetStreamStatRequest{StreamID: item.StreamId})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return 0, 0, nil
		}
		return 0, 0, errors.WithStack(err)
	}
	return stat.Begin, stat.End, nil
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
		return nil, errors.WithStack(err)
	}
	conn, err := grpc.DialContext(ctx, response.Base.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	client := proto.NewStreamServiceClient(conn)
	c.streamServiceClient[streamServerID] = client
	return client, nil
}

func (c *client) NewStreamSession(ctx context.Context, sessionID int64, streamInfo *proto.StreamInfoItem) (StreamSession, error) {
	metaServiceClient, err := c.getMetaServiceClient()
	if err != nil {
		return nil, err
	}
	return &session{
		readBuffSize:     1024 * 4,
		ctx:              ctx,
		sessionID:        sessionID,
		client:           c,
		streamInfo:       streamInfo,
		metaServerClient: metaServiceClient,
	}, nil
}

func (c *client) Close() error {
	return nil
}

type session struct {
	readBuffSize     int64
	ctx              context.Context
	sessionID        int64
	client           *client
	streamInfo       *proto.StreamInfoItem
	metaServerClient proto.MetaServiceClient
}

func (s *session) NewReader() (StreamReader, error) {
	offset, err := s.GetReadOffset()
	if err != nil {
		errStatus := status.Convert(err)
		if errStatus.Code() != codes.NotFound {
			return nil, errors.WithStack(err)
		}
	}
	streamClient, err := s.client.getStreamClient(s.ctx, s.streamInfo.StreamServerId)
	if err != nil {
		return nil, err
	}
	return &streamReader{
		readBuffSize:    s.readBuffSize,
		offset:          offset,
		rpcStreamReader: nil,
		reader:          nil,
		client:          streamClient,
		ctx:             s.ctx,
		session:         s,
	}, nil
}

func (s *session) NewWriter() (StreamWriter, error) {
	streamServiceClient, err := s.client.getStreamClient(s.ctx, s.streamInfo.StreamServerId)
	if err != nil {
		return nil, err
	}
	writeStreamRequests := s.client.startStreamRequestWriter(s.streamInfo.StreamServerId, streamServiceClient)
	return &streamWriter{
		locker:     sync.Mutex{},
		streamInfo: s.streamInfo,
		buffer:     bytes.Buffer{},
		queue:      writeStreamRequests,
	}, nil
}

var chanPool = sync.Pool{New: func() interface{} { return make(chan interface{}, 1) }}

func (s *session) SetReadOffset(offset int64) error {
	var err error
	ch := chanPool.Get().(chan interface{})
	s.client.putSetReadOffsetRequest(setReadOffsetRequest{
		item: &proto.SSOffsetItem{Offset: offset, SessionId: s.sessionID, StreamId: s.streamInfo.StreamId},
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

func (s *session) SetReadOffsetWithCb(offset int64, f func(err error)) {
	s.client.putSetReadOffsetRequest(setReadOffsetRequest{
		item: &proto.SSOffsetItem{Offset: offset, SessionId: s.sessionID, StreamId: s.streamInfo.StreamId},
		cb:   f,
	})
}

func (s *session) GetReadOffset() (offset int64, err error) {
	response, err := s.metaServerClient.GetStreamReadOffset(s.ctx,
		&proto.GetStreamReadOffsetRequest{StreamId: s.streamInfo.StreamId, SessionId: s.sessionID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	return response.SSOffset.Offset, nil
}
