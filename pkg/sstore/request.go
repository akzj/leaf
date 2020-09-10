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
	"encoding/binary"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/golang/protobuf/proto"
	"io"
	"sync"
)

type closeRequest struct {
	cb func()
}

type writeRequest struct {
	entry *pb.Entry
	close bool
	end   int64
	err   error
	cb    func(end int64, err error)
}

var objsPool = sync.Pool{New: func() interface{} {
	return make([]interface{}, 0, 64)
}}

func (e *writeRequest) WriteTo(w io.Writer) (n int64, err error) {
	data, err := proto.Marshal(e.entry)
	if err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, int32(len(data))); err != nil {
		return 0, err
	}
	n2, err := w.Write(data)
	return int64(n2), err
}

func decodeEntry(reader io.Reader) (*writeRequest, error) {
	var size int32
	if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}
	var entry pb.Entry
	if err := proto.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &writeRequest{
		entry: &entry,
		close: false,
		end:   0,
		err:   nil,
		cb:    nil,
	}, nil
}
