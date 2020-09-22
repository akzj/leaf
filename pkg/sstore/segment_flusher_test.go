package sstore

import (
	"bytes"
	"context"
	block_queue "github.com/akzj/streamIO/pkg/block-queue"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSegmentFlusher(t *testing.T) {

	var entryCount int = 50000
	var streamCount = 100
	var index int64
	var dataBuffer bytes.Buffer

	os.MkdirAll("data/segments", 0777)
	defer os.RemoveAll("data")

	int64Map := newInt64LockMap()
	table := newStreamTable(int64Map, 4*1024, 0)

	data := []byte(strings.Repeat("stream store test\n", 10))

	t.Run("append_entry", func(t *testing.T) {
		for i := 0; i < entryCount; i++ {
			index++
			dataBuffer.Write(data)
			for streamID := 0; streamID < streamCount; streamID++ {
				_, err := table.appendEntry(&pb.Entry{
					StreamID: int64(streamID),
					Offset:   -1,
					Data:     data,
					Ver:      &pb.Version{Index: index},
				}, nil)
				assert.NoError(t, err)
			}
		}
	})

	var filename = "data/segments/1.seg"
	t.Run("flushSegment", func(t *testing.T) {
		queue := block_queue.NewQueueWithContext(context.Background(), 1)
		flusher := newSegmentFlusher("data/segments", queue)
		go flusher.flushLoop()

		var wg sync.WaitGroup
		wg.Add(1)
		err := flusher.queue.Push(flushSegment{
			rename:      filename,
			streamTable: table,
			callback: func(filename string, err error) {
				assert.NoError(t, err)
				wg.Done()
			},
		})
		assert.NoError(t, err)
		wg.Wait()
		queue.Close(nil)
	})

	var segment *segment
	t.Run("open_segment", func(t *testing.T) {
		var err error
		segment, err = openSegment(filename)
		assert.NoError(t, err)
	})

	t.Run("read_segment", func(t *testing.T) {
		for streamID := 0; streamID < streamCount; streamID++ {
			reader := segment.Reader(int64(streamID))
			assert.NotNil(t, reader)
			data, err := ioutil.ReadAll(reader)
			assert.NoError(t, err)
			assert.Equal(t, bytes.Compare(data, dataBuffer.Bytes()), 0)
		}
	})

	t.Run("segment_close", func(t *testing.T) {
		segment.IncRef()
		assert.NoError(t, segment.deleteOnClose(true))
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			go func() {
				if segment.IncRef() > 0 {
					wg.Add(1)
					defer func() {
						segment.DecRef()
						wg.Done()
					}()
					time.Sleep(time.Millisecond * time.Duration(rand.Int31n(100)))
				}
			}()
		}
		time.Sleep(time.Millisecond * 50)
		segment.DecRef()
		wg.Wait()

		_, err := os.Stat(filename)
		assert.Error(t, err)
	})

}
