package sstore

import (
	"bytes"
	"context"
	"fmt"
	block_queue "github.com/akzj/streamIO/pkg/block-queue"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestSectionsTable(t *testing.T) {

	var dataBuffer bytes.Buffer

	os.MkdirAll("data/segments", 0777)
	defer os.RemoveAll("data")

	var segments []*segment
	var streamTables []*streamTable
	var streamCount = 100
	t.Run("flushSegment", func(t *testing.T) {
		var entryCount int = 50
		var segmentCount = 100
		var index int64
		int64Map := newInt64LockMap()
		data := []byte(strings.Repeat("stream store test\n", 10))

		queue := block_queue.NewQueueWithContext(context.Background(), 1)
		flusher := newSegmentFlusher("data/segments", queue)
		go flusher.flushLoop()

		var wg sync.WaitGroup
		for segmentID := 1; segmentID <= segmentCount; segmentID++ {
			wg.Add(1)
			table := newStreamTable(int64Map, 4*1024, 0)
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
			streamTables = append(streamTables, table)
			var filename = fmt.Sprintf("data/segments/%d.seg", segmentID)
			err := flusher.queue.Push(flushSegment{
				rename:      filename,
				streamTable: table,
				callback: func(filename string, err error) {
					assert.NoError(t, err)
					wg.Done()
					segment, err := openSegment(filename)
					assert.NoError(t, err)
					segment.IncRef()
					segments = append(segments, segment)
				},
			})
			assert.NoError(t, err)
		}
		wg.Wait()
		queue.Close(nil)
	})

	table := newSectionsTable()

	t.Run("UpdateSectionWithStream", func(t *testing.T) {
		for i := 0; i < streamCount; i++ {
			for _, stable := range streamTables {
				stream, load := stable.loadOrCreateStream(int64(i))
				assert.True(t, load)
				table.UpdateSectionWithStream(stream)
			}
		}
	})

	t.Run("read_stream_from_section_table", func(t *testing.T) {
		for i := 0; i < streamCount; i++ {
			reader, err := table.Reader(int64(i))
			assert.NoError(t, err)
			data, err := ioutil.ReadAll(reader)
			assert.NoError(t, err)
			assert.Equal(t, bytes.Compare(data, dataBuffer.Bytes()), 0)
		}
	})

	t.Run("remove_section_with_stream", func(t *testing.T) {
		for i := 0; i < streamCount; i++ {
			for index, stable := range streamTables {
				last := index == len(streamTables)-1
				stream, load := stable.loadOrCreateStream(int64(i))
				assert.True(t, load)
				table.RemoveSectionWithStream(stream)
				sections, ok := table.sectionsMap[int64(i)]
				if last == false {
					assert.True(t, ok)
					section, _, err := sections.find(stream.begin)
					assert.NoError(t, err)
					assert.True(t, section.begin > stream.begin, fmt.Sprintf("%d %d %d %d",
						section.begin, section.end, stream.begin, stream.end))
				} else {
					assert.False(t, ok)
				}
			}
		}
	})

	t.Run("update_section_with_segment", func(t *testing.T) {
		for i := 0; i < streamCount; i++ {
			for _, segment := range segments {
				_, ok := segment.meta.SectionOffsets[int64(i)]
				assert.True(t, ok)
			}
		}
		for _, segment := range segments {
			assert.NoError(t, table.UpdateSectionWithSegment(segment))
		}
	})

	t.Run("read_segment_from_section_table", func(t *testing.T) {
		for i := 0; i < streamCount; i++ {
			reader, err := table.Reader(int64(i))
			assert.NoError(t, err)
			data, err := ioutil.ReadAll(reader)
			assert.NoError(t, err)
			assert.Equal(t, bytes.Compare(data, dataBuffer.Bytes()), 0)
		}
	})

	t.Run("remove_section_with_segment", func(t *testing.T) {
		for _, segment := range segments {
			segment.DecRef()
			assert.NoError(t, segment.deleteOnClose(true))
			assert.NoError(t, table.removeSectionWithSegment(segment))
			assert.True(t, segment.Count() < 0, fmt.Sprintf("%s %d", segment.filename, segment.Count()))
			_, err := os.Stat(segment.filename)
			assert.Error(t, err)
		}
	})

}
