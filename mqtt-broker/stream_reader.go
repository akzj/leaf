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

package mqtt_broker

import (
	"context"
	"github.com/akzj/streamIO/client"
	"github.com/akzj/streamIO/proto"
	"github.com/eclipse/paho.mqtt.golang/packets"
	log "github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	"time"
)

type Offset interface {
	commitOffset()
}

type streamPacketReader struct {
	ackMapLocker    sync.Mutex
	ackMap          map[uint16]int64
	Reader          client.StreamReader
	streamSession   client.StreamSession
	AckOffset       int64
	Offset          int64
	committedOffset int64
	Qos             int32
	sess            *session

	ctx    context.Context
	cancel context.CancelFunc
}

func newStreamPacketReader(ctx context.Context, sess *session,
	item *proto. StreamInfoItem, qos int32, client client.Client, offsetCommitter *offsetCommitter) (*streamPacketReader, error) {
	ctx, cancel := context.WithCancel(ctx)
	streamSession, reader, err := client.CreateSessionAndReader(ctx, sess.MQTTSessionInfo.SessionId, item)
	if err != nil {
		return nil, err
	}
	streamPacketReader := &streamPacketReader{
		ackMapLocker:    sync.Mutex{},
		ackMap:          map[uint16]int64{},
		Reader:          reader,
		streamSession:   streamSession,
		AckOffset:       reader.Offset(),
		Offset:          reader.Offset(),
		committedOffset: reader.Offset(),
		Qos:             qos,
		sess:            sess,
		ctx:             ctx,
	}
	offsetCommitter.AddCommitOffset(streamPacketReader)
	var isCancel int32
	streamPacketReader.cancel = func() {
		if atomic.CompareAndSwapInt32(&isCancel, 0, 1) {
			offsetCommitter.Delete(streamPacketReader)
			cancel()
		}
	}
	return streamPacketReader, nil
}

func (spp *streamPacketReader) commitOffset() {
	ackOffset := atomic.LoadInt64(&spp.AckOffset)
	if ackOffset == atomic.LoadInt64(&spp.committedOffset) {
		return
	}
	spp.streamSession.SetReadOffsetWithCb(ackOffset, func(err error) {
		if err == nil {
			atomic.StoreInt64(&spp.AckOffset, ackOffset)
		} else {
			log.Errorf("%+v", err)
			_ = spp.sess.Close()
		}
	})
}

func (spp *streamPacketReader) readPacketLoop() error {
	defer func() {
		_ = spp.Reader.Close()
		spp.cancel()
		spp.commitOffset()
	}()
	for {
		controlPacket, err := packets.ReadPacket(spp.Reader)
		if err != nil {
			select {
			case <-spp.ctx.Done():
				spp.sess.log.Infof("readPacketLoop qos %d done", spp.Qos)
				return nil
			default:
				return err
			}
		}
		spp.Offset = spp.Reader.Offset()
		switch packet := controlPacket.(type) {
		case *packets.PublishPacket:
			if packet.Qos == 0 {
				atomic.StoreInt64(&spp.AckOffset, spp.Offset)
			} else if packet.Qos == 1 {
				spp.ackMapLocker.Lock()
				spp.ackMap[packet.MessageID] = spp.Offset
				spp.ackMapLocker.Unlock()
			}
			if err := spp.sess.handleOutPublishPacket(packet); err != nil {
				return err
			}
		}
	}
}

func (spp *streamPacketReader) Close() {
	spp.cancel()
}

func (spp *streamPacketReader) handleAck(id uint16) {
	spp.ackMapLocker.Lock()
	if offset, _ := spp.ackMap[id]; offset > atomic.LoadInt64(&spp.AckOffset) {
		atomic.StoreInt64(&spp.AckOffset, offset)
	}
	spp.ackMapLocker.Unlock()
}

type offsetCommitter struct {
	locker  sync.Mutex
	offsets map[Offset]struct{}
}

func newOffsetCommitter() *offsetCommitter {
	return &offsetCommitter{
		locker:  sync.Mutex{},
		offsets: map[Offset]struct{}{},
	}
}

func (committer *offsetCommitter) AddCommitOffset(offset Offset) {
	committer.locker.Lock()
	defer committer.locker.Unlock()
	committer.offsets[offset] = struct{}{}
}

func (committer *offsetCommitter) Delete(offset Offset) {
	committer.locker.Lock()
	defer committer.locker.Unlock()
	delete(committer.offsets, offset)
}

func (committer *offsetCommitter) commitLoop(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		committer.locker.Lock()
		for offset := range committer.offsets {
			offset.commitOffset()
		}
		committer.locker.Unlock()
	}
}
