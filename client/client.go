package client

import (
	"bufio"
	"context"
	"fmt"
	"github.com/akzj/streamIO/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"sync/atomic"
	"unsafe"
)

type ReadSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

type StreamSession interface {
	NewReader() (ReadSeekCloser, error)
	NewWriter() (io.WriteCloser, error)
	SetReadOffset(offset int64) error
	GetReadOffset() (offset int64, err error)
}

type Client interface {
	CreateStream(ctx context.Context, name string) (streamID int64, err error)
	GetStreamID(ctx context.Context, name string) (streamID int64, err error)
	GetOrCreateStream(ctx context.Context, name string) (streamID int64, err error)
	NewStreamSession(ctx context.Context, sessionID int64, name string) (StreamSession, error)
}

type client struct {
	err                   error
	metaServerClient      proto.MetaServiceClient
	streamServiceClient   proto.StreamServiceClient
	streamServiceClientWG unsafe.Pointer
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

func NewClient(MetaServer string) Client {
	return &client{
	}
}

func (c *client) getMetaServiceClient() (proto.MetaServiceClient, error) {
	return c.metaServerClient, nil
}

func (c *client) CreateStream(ctx context.Context, name string) (streamID int64, err error) {
	msClient, err := c.getMetaServiceClient()
	if err != nil {
		return 0, err
	}

	response, err := msClient.CreateStream(ctx, &proto.CreateStreamRequest{Name: name})
	if err != nil {
		return 0, err
	}
	return response.Info.StreamId, nil
}

func (c *client) GetStreamID(ctx context.Context, name string) (streamID int64, err error) {
	msClient, err := c.getMetaServiceClient()
	if err != nil {
		return 0, err
	}

	response, err := msClient.CreateStream(ctx, &proto.CreateStreamRequest{Name: name})
	if err != nil {
		return 0, err
	}
	return response.Info.StreamId, nil
}

func (c *client) GetOrCreateStream(ctx context.Context, name string) (streamID int64, err error) {
	msClient, err := c.getMetaServiceClient()
	if err != nil {
		return 0, err
	}

	response, err := msClient.GetOrCreateStream(ctx, &proto.GetStreamInfoRequest{Name: name})
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
	msClient, err := c.getMetaServiceClient()
	if err != nil {
		return nil, err
	}
	return &session{
		name:             name,
		readBuffSize:     0,
		ctx:              ctx,
		sessionID:        sessionID,
		streamID:         streamID,
		client:           c,
		metaServerClient: msClient,
	}, nil
}

func (c *client) getStreamClient(streamServerID int64) (proto.StreamServiceClient, error) {
	var wg sync.WaitGroup
	wg.Add(1)
	if atomic.CompareAndSwapPointer(&c.streamServiceClientWG, nil, unsafe.Pointer(&wg)) == false {
		if obj := atomic.LoadPointer(&c.streamServiceClientWG); obj != nil {
			(*sync.WaitGroup)(obj).Wait()
		}
		return c.streamServiceClient, c.err
	}
	response, err := c.metaServerClient.ListStreamServer(context.Background(), &empty.Empty{})
	if err != nil {
		c.err = err
		atomic.StorePointer(&c.streamServiceClientWG, nil)
		return nil, err
	}
	//todo
	return nil,nil
}

func (s *session) NewReader() (ReadSeekCloser, error) {
	response, err := s.metaServerClient.GetStreamInfo(s.ctx, &proto.GetStreamInfoRequest{Name: s.name})
	if err != nil {
		return nil, err
	}
	var offset int64
	response2, err := s.metaServerClient.GetStreamReadOffset(s.ctx,
		&proto.GetStreamReadOffsetRequest{
			StreamId:  response.Info.StreamId,
			SessionId: s.sessionID})
	streamID := response.Info.StreamId
	if err != nil {
		errStatus := status.Convert(err)
		if errStatus.Code() != codes.NotFound {
			return nil, err
		}
	} else {
		offset = response2.SSOffset.Offset
	}
	streamClient, err := s.client.getStreamClient(response.Info.StreamServerId)
	if err != nil {
		return nil, err
	}
	return &streamReader{
		readBuffSize: s.readBuffSize,
		offset:       offset,
		streamID:     streamID,
		client:       streamClient,
	}, nil
}

func (s *session) NewWriter() (io.WriteCloser, error) {
	response, err := s.metaServerClient.GetStreamInfo(s.ctx, &proto.GetStreamInfoRequest{Name: s.name})
	if err != nil {
		return nil, err
	}
	streamServiceClient, err := s.client.getStreamClient(response.Info.StreamServerId)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(s.ctx)
	return &streamWriter{
		streamID:  response.Info.StreamId,
		ctx:       ctx,
		stop:      cancel,
		client:    streamServiceClient,
		offset:    -1,
		requestID: 0,
	}, nil
}

func (s *session) SetReadOffset(offset int64) error {
	panic("implement me")
}

func (s *session) GetReadOffset() (offset int64, err error) {
	panic("implement me")
}

type streamReader struct {
	readBuffSize    int64
	offset          int64
	streamID        int64
	rpcStreamReader *rpcStreamReader
	reader          *bufio.Reader
	client          proto.StreamServiceClient
	ctx             context.Context
}

func (s *streamReader) Seek(offset int64, whence int) (int64, error) {
	stat, err := s.client.GetStreamStat(s.ctx, &proto.GetStreamStatRequest{StreamID: s.streamID})
	if err != nil {
		return 0, err
	}
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += s.offset
	case io.SeekEnd:
		offset += stat.End
	}
	if offset > stat.End || offset < stat.Begin {
		return 0, fmt.Errorf("offset out of stream range [%d,%d]", stat.Begin, stat.End)
	}
	if s.offset != offset {
		s.reset()
	}
	s.offset = offset
	return s.offset, nil
}

func (s *streamReader) reset() {
	_ = s.rpcStreamReader.Close()
	s.rpcStreamReader = nil
}

func (s *streamReader) getBufReader() *bufio.Reader {
	if s.reader != nil {
		return s.reader
	}
	if s.rpcStreamReader != nil {
		_ = s.rpcStreamReader.Close()
	}
	s.rpcStreamReader = newRpcStreamReader(s.ctx, s.streamID,
		s.offset, s.readBuffSize, s.client)
	s.reader = bufio.NewReader(s.rpcStreamReader)
	return s.reader
}

func (s *streamReader) Read(p []byte) (n int, err error) {
	return s.getBufReader().Read(p)
}

func (s *streamReader) Close() error {
	s.reset()
	return nil
}

type rpcStreamReader struct {
	streamID    int64
	offset      int64
	bytesToRead int64
	notify      chan interface{}
	ctx         context.Context
	stop        context.CancelFunc
	client      proto.StreamServiceClient
	responses   chan []byte
	minToRead   int64

	readBuffer []byte
}

func newRpcStreamReader(ctx context.Context,
	streamID int64,
	offset int64,
	minToRead int64,
	client proto.StreamServiceClient) *rpcStreamReader {
	reader := &rpcStreamReader{
		streamID:    streamID,
		offset:      offset,
		bytesToRead: 0,
		notify:      make(chan interface{}, 1),
		client:      client,
		minToRead:   minToRead,
		responses:   make(chan []byte, 1),
	}
	reader.ctx, reader.stop = context.WithCancel(ctx)
	go reader.rpcRequestLoop()
	return reader
}

func (r *rpcStreamReader) rpcRequestLoop() {
	for {
		toRead := atomic.LoadInt64(&r.bytesToRead)
		if toRead < 0 {
			select {
			case <-r.notify:
				continue
			case <-r.ctx.Done():
				return
			}
		}
		if toRead < r.minToRead {
			toRead = r.minToRead
		}
		stream, err := r.client.ReadStream(r.ctx, &proto.ReadStreamRequest{
			StreamId: r.streamID,
			Offset:   r.offset,
			Size:     toRead,
		})
		if err != nil {
			return
		}
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				return
			}
			r.offset += int64(len(response.Data))
			atomic.AddInt64(&r.bytesToRead, -int64(len(response.Data)))
			select {
			case r.responses <- response.Data:
			case <-r.ctx.Done():
				//notify server close stream
				_ = stream.CloseSend()
				return
			}
		}
	}
}

func (r *rpcStreamReader) Read(p []byte) (n int, err error) {
	var size int
	atomic.AddInt64(&r.bytesToRead, int64(len(p)))
	select {
	case <-r.notify:
	default:
	}
	for len(p) > 0 {
		if r.readBuffer != nil {
			n := copy(p, r.readBuffer)
			size += n
			p = p[n:]
			r.readBuffer = r.readBuffer[n:]
			if len(r.readBuffer) == 0 {
				r.readBuffer = nil
			}
		} else {
			if size > 0 {
				return size, nil
			}
			select {
			case r.readBuffer = <-r.responses:
				continue
			case <-r.ctx.Done():
				return size, r.ctx.Err()
			}
		}
	}
	return size, nil
}

func (r *rpcStreamReader) Close() error {
	r.stop()
	return nil
}

type streamWriter struct {
	streamID      int64
	ctx           context.Context
	stop          context.CancelFunc
	client        proto.StreamServiceClient
	offset        int64
	requestSender proto.StreamService_WriteStreamClient
	requestID     int64
}

func (s *streamWriter) Write(p []byte) (n int, err error) {
	if s.requestSender == nil {
		var err error
		s.requestSender, err = s.client.WriteStream(s.ctx)
		if err != nil {
			return 0, err
		}
	}
	err = s.requestSender.Send(&proto.WriteStreamRequest{
		StreamId:  s.streamID,
		Offset:    s.offset,
		Data:      p,
		RequestId: atomic.AddInt64(&s.requestID, 1),
	})
	if err != nil {
		return 0, err
	}
	response, err := s.requestSender.Recv()
	if err != nil {
		return 0, err
	}
	s.offset = response.Offset
	return len(p), nil
}

func (s *streamWriter) Close() error {
	if s.requestSender != nil {
		return s.requestSender.CloseSend()
	}
	return nil
}
