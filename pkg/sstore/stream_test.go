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

package sstore

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
	"io"
	"testing"
)

func TestStream(t *testing.T) {

	var offset int64 = 107
	var pageSize int = 128
	var streamID int64 = 1

	makeData := func(size int) []byte {
		data := make([]byte, size)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		return data
	}

	stream := newStream(offset, pageSize, streamID)

	var buffer bytes.Buffer

	t.Run("write_offset_error", func(t *testing.T) {
		data := makeData(1)
		_, err := stream.WriteAt(data, offset+1)
		assert.Error(t, err)
	})

	writeCases := []struct {
		offset int64
		size   int
		end    int64
	}{
		{
			offset: offset,
			size:   1,
		},
		{
			offset: offset + 1,
			size:   pageSize,
		},
		{
			offset: offset + 1 + int64(pageSize),
			size:   1024 * 1024,
		},
		{
			offset: offset + 1 + int64(pageSize+1024*1024),
			size:   1,
		},
	}

	t.Run("Write", func(t *testing.T) {
		for _, C := range writeCases {
			data := makeData(C.size)
			n, err := stream.WriteAt(data, C.offset)
			assert.NoError(t, err)
			buffer.Write(data)
			assert.Equal(t, n, len(data))
			offset += int64(n)
		}
	})

	t.Run("WriteTo", func(t *testing.T) {
		var reader bytes.Buffer
		_, err := stream.WriteTo(&reader)
		assert.NoError(t, err)
		assert.Equal(t, bytes.Compare(reader.Bytes(), buffer.Bytes()), 0)
	})

	t.Run("Reader", func(t *testing.T) {

		_, err := stream.ReadAt(nil, 1)
		assert.Error(t, err)
		_, err = stream.ReadAt(nil, int64(len(buffer.Bytes())))
		assert.Equal(t, err, io.EOF)

		data := make([]byte, int(float64(pageSize)*1.5))
		offset := stream.begin + 33
		n, err := stream.ReadAt(data, offset)
		assert.NoError(t, err)
		assert.Equal(t, len(data), n)

		assert.Equal(t, bytes.Compare(data, buffer.Bytes()[33:33+len(data)]), 0)
	})
}
