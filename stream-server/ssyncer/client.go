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
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	sstore *sstore.Store
	ctx    context.Context
	cancel context.CancelFunc
}

func NewClient(store *sstore.Store) *Client {
	return &Client{
		sstore: store,
		ctx:    context.Background(),
		cancel: func() {},
	}
}

func (c *Client) Stop() {
	c.cancel()
}

var entryIndex int64

func (c *Client) Start(ctx context.Context, localStreamServiceID int64, serviceAddr string) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	conn, err := grpc.DialContext(c.ctx, serviceAddr, grpc.WithInsecure())
	if err != nil {
		return errors.WithStack(err)
	}
	cc := proto.NewSyncServiceClient(conn)
	var segmentWriter *sstore.SegmentReceiver
	defer func() {
		if segmentWriter != nil {
			_ = segmentWriter.Discard()
		}
	}()

	var segmentName string
	var count int64
	var lCount int64
	go func() {
		for {
			temp := atomic.LoadInt64(&count)
			if msg := temp - lCount; msg > 0 {
				fmt.Println("msg/second", msg)
			}
			lCount = temp
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()
	for {
		index := c.sstore.Version()
		stream, err := cc.SyncRequest(c.ctx, &proto.SyncRequest{
			Index:          index.Index + 1,
			StreamServerId: localStreamServiceID,
		})
		if err != nil {
			log.Warn(err)
			select {
			case <-c.ctx.Done():
				return c.ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}
		hash := md5.New()
		var wg sync.WaitGroup
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				log.Warn(err)
				break
			}
			if err != nil {
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
			} else if response.Entries != nil {
				for _, entry := range response.Entries {
					if entryIndex == 0 {
						entryIndex = entry.Ver.Index
					} else {
						entryIndex++
						if entryIndex != entry.Ver.Index {
							fmt.Println(entryIndex, entry.Ver.Index)
							entryIndex = entry.Ver.Index
						}
					}
					atomic.AddInt64(&count, 1)
					wg.Add(1)
					c.sstore.AppendEntryWithCb(entry, func(cbError error) {
						wg.Done()
						if cbError != nil {
							log.Warn(cbError)
							panic(err)
						}
					})
				}
			}
		}
		wg.Wait()
	}
}
