package client

import (
	"bufio"
	"context"
	"fmt"
	"github.com/akzj/streamIO/proto"
	"io"
)

type StreamSession interface {
	NewReader(streamID string) (io.ReadSeeker, error)
	NewWriter(streamID string) (io.WriteCloser, error)
	SetReadOffset(offset int64) error
	GetReadOffset() (offset int64, err error)
}

type Client interface {
	CreateStream(ctx context.Context, name string) (streamID int64, err error)
	GetStreamID(ctx context.Context, name string) (streamID int64, err error)
	GetOrCreateStream(ctx context.Context, name string) (streamID int64, err error)
	NewStreamSession(ctx context.Context, sessionID int64, streamID int64) (StreamSession, error)
}

type client struct {
	metaServerClient proto.MetaServiceClient
}

type session struct {
	ctx              context.Context
	sessionID        int64
	streamID         int64
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

func (c *client) NewStreamSession(ctx context.Context, sessionID int64, streamID int64) (StreamSession, error) {
	msClient, err := c.getMetaServiceClient()
	if err != nil {
		return nil, err
	}
	return &session{
		sessionID:        sessionID,
		streamID:         streamID,
		metaServerClient: msClient,
	}, nil
}

func (s *session) NewReader(streamID string) (io.ReadSeeker, error) {
	panic("implement me")
}

func (s *session) NewWriter(streamID string) (io.WriteCloser, error) {
	panic("implement me")
}

func (s *session) SetReadOffset(offset int64) error {
	panic("implement me")
}

func (s *session) GetReadOffset() (offset int64, err error) {
	panic("implement me")
}

type streamReader struct {
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

}

func (s *streamReader) getBufReader() *bufio.Reader {
	if s.reader != nil {
		return s.reader
	}
	if s.rpcStreamReader != nil {
		_ = s.rpcStreamReader.Close()
	}
	s.rpcStreamReader = &rpcStreamReader{
		client: s.client,
		offset: s.offset,
	}
	s.reader = bufio.NewReader(s.rpcStreamReader)
	return s.reader
}


func (s *streamReader) Read(p []byte) (n int, err error) {
	return s.getBufReader().Read(p)
}

func (s *streamReader) Close() error {
	panic("implement me")
}

type rpcStreamReader struct {
	client proto.StreamServiceClient
	offset int64
}

func (r *rpcStreamReader) Read(p []byte) (n int, err error) {
	panic("implement me")
}

func (r *rpcStreamReader) Close() error {
	panic("implement me")
}

type streamWriter struct {
	client proto.StreamServiceClient
}

func (s *streamWriter) Write(p []byte) (n int, err error) {
	panic("implement me")
}

func (s *streamWriter) Close() error {
	panic("implement me")
}
