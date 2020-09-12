package ssyncer

import (
	"context"
	"github.com/akzj/streamIO/pkg/sstore"
	"github.com/akzj/streamIO/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"time"
)

type Client struct {
	sstore *sstore.SStore
	ctx    context.Context
	cancel context.CancelFunc
}

func NewClient(store *sstore.SStore) *Client {
	return &Client{
		sstore: store,
		ctx:    context.Background(),
		cancel: func() {},
	}
}

func (c *Client) Stop() {
	c.cancel()
}

func (c *Client) Start(ctx context.Context, serviceAddr string) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	conn, err := grpc.DialContext(c.ctx, serviceAddr, grpc.WithInsecure())
	if err != nil {
		return errors.WithStack(err)
	}
	cc := proto.NewSyncServiceClient(conn)

	for {
		stream, err := cc.SyncRequest(c.ctx, &proto.SyncRequest{
			Index:              0,
			StreamServerId:     0,
			SyncSegmentRequest: nil,
		})
		if err != nil {
			select {
			case <-c.ctx.Done():
				return c.ctx.Err()
			case <-time.After(time.Second):
			}
		}
		var segmentWriter *sstore.SegmentWriter
		for {
			response, err := stream.Recv()
			if err == nil {
				if segmentWriter != nil {
					if err := segmentWriter.Discard(); err != nil {
						return err
					}
				}
				if err != nil {
					log.Error(err)
				}
				break
			}
			if response.SegmentInfo != nil {
				segmentWriter, err = c.sstore.CreateSegmentWriter(response.SegmentInfo.Name)
				if err != nil {
					log.Error(err)
					return err
				}
			} else if response.SegmentData != nil {
				if segmentWriter.Offset() != response.SegmentData.Offset {
					log.Error("offset error")
					return errors.Errorf("offset error")
				}
				if _, err := segmentWriter.Write(response.SegmentData.Data); err != nil {
					log.Errorf(err.Error())
					return err
				}
			} else if response.SegmentEnd != nil {
				if err := segmentWriter.Commit(); err != nil {
					log.Error(err.Error())
					return err
				}
				segmentWriter = nil
			} else if response.Entry != nil {
				c.sstore.AppendEntryWithCb(response.Entry, func(offset int64, err error) {
					if err != nil {
						log.Warn(err)
					}
				})
			}
		}
	}
}
