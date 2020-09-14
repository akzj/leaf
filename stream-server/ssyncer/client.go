package ssyncer

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/akzj/streamIO/pkg/sstore"
	"github.com/akzj/streamIO/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io"
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

func (c *Client) Start(ctx context.Context, localStreamServiceID int64, serviceAddr string) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	conn, err := grpc.DialContext(c.ctx, serviceAddr, grpc.WithInsecure())
	if err != nil {
		return errors.WithStack(err)
	}
	cc := proto.NewSyncServiceClient(conn)
	var segmentWriter *sstore.SegmentWriter
	defer func() {
		if segmentWriter != nil {
			segmentWriter.Discard()
		}
	}()
	for {
		index := c.sstore.Version()
		stream, err := cc.SyncRequest(c.ctx, &proto.SyncRequest{
			Index:          index.Index,
			StreamServerId: localStreamServiceID,
		})
		if err != nil {
			select {
			case <-c.ctx.Done():
				return c.ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}
		hash := md5.New()
		var segmentName string
		var appendEntryError bool
		for !appendEntryError {
			response, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err == nil {
				log.Error(err.Error())
				break
			}
			if response.SegmentInfo != nil {
				if segmentWriter != nil {
					log.Panic("last segment is commit or discard")
				}
				segmentWriter, err = c.sstore.CreateSegmentWriter(response.SegmentInfo.Name)
				if err != nil {
					log.Error(err)
					return err
				}
				segmentName = response.SegmentInfo.Name
			} else if response.SegmentData != nil {
				if segmentWriter == nil {
					log.Panic("segment not open")
				}
				hash.Write(response.SegmentData.Data)
				if _, err = segmentWriter.Write(response.SegmentData.Data); err != nil {
					log.Errorf(err.Error())
					return err
				}
			} else if response.SegmentEnd != nil {
				if segmentWriter == nil {
					log.Panic("segment writer not open")
				}
				sum := fmt.Sprintf("%x", hash.Sum(nil))
				if response.SegmentEnd.Md5Sum != sum {
					err = errors.Errorf("sync segment %s failed,md5 sum error %s %s",
						segmentName, response.SegmentEnd.Md5Sum, sum)
					log.Error(err)
					return err
				}
				if err = segmentWriter.Commit(); err != nil {
					log.Error(err.Error())
					return err
				}
				segmentWriter = nil
			} else if response.Entry != nil {
				c.sstore.AppendEntryWithCb(response.Entry, func(offset int64, cbError error) {
					if cbError != nil {
						log.Warn(cbError)
						appendEntryError = true
					}
				})
			}
		}
	}
}
