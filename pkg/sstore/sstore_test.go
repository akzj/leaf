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
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	log.SetReportCaller(true)
	assert.NoError(t, os.RemoveAll("data"))
	defer func() {
		assert.NoError(t, os.RemoveAll("data"))
	}()

	sstore, err := Open(DefaultOptions("data").
		WithMaxMStreamTableSize(MB / 2).
		WithMaxWalSize(MB))
	assert.NoError(t, err)

	var streamID = int64(1)
	var data = []byte(strings.Repeat("hello world,", 30))
	var wg sync.WaitGroup
	var hash32 = crc32.NewIEEE()

	var size int64
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		_, _ = hash32.Write(data)
		size += int64(len(data))
		sstore.AsyncAppend(streamID, data, -1, func(pos int64, err error) {
			assert.NoError(t, err)
			wg.Done()
		})
	}
	wg.Wait()

	sum32 := hash32.Sum32()

	for index, segment := range sstore.sectionsTable.Get(streamID).sections {
		last := index == len(sstore.sectionsTable.Get(streamID).sections)-1
		s, l, err := sstore.sectionsTable.Get(streamID).Find(segment.begin)
		assert.NoError(t, err)
		assert.Equal(t, s, segment)
		assert.Equal(t, last, l)
	}
	_, last, err := sstore.sectionsTable.Get(streamID).Find(size)
	assert.NoError(t, err)
	assert.True(t, last)

	for i := int64(0); i < size; i++ {
		info, last, err := sstore.sectionsTable.Get(streamID).Find(i)
		assert.NoError(t, err)
		if last == false {
			assert.True(t, info.begin <= i && i < info.end, fmt.Sprintf("info[%d %d) %d %d", info.begin, info.end, i, size))
		}
	}

	var buffer = make([]byte, size)
	reader, err := sstore.Reader(streamID)
	assert.NoError(t, err)
	n, err := reader.Read(buffer)
	assert.NoError(t, err)
	assert.True(t, n == len(buffer))

	hash32 = crc32.NewIEEE()
	hash32.Write(buffer)
	assert.True(t, hash32.Sum32() == sum32)

	reader, _ = sstore.Reader(streamID)
	readAllData, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	hash32 = crc32.NewIEEE()
	hash32.Write(readAllData)
	assert.True(t, hash32.Sum32() == sum32)
	assert.Nil(t, sstore.Close())
}

func TestSStore_Watcher(t *testing.T) {
	os.RemoveAll("data")
	defer os.RemoveAll("data")
	sstore, err := Open(DefaultOptions("data").WithMaxMStreamTableSize(MB))
	if err != nil {
		t.Fatalf("%+v", err)
	}

	var streamID = int64(1)
	var data = "hello world"
	var wg sync.WaitGroup
	wg.Add(1)
	sstore.AsyncAppend(streamID, []byte(data), int64(-1), func(offset int64, err error) {
		defer wg.Done()
		if err != nil {
			t.Fatalf("%+v", err)
		}
	})
	wg.Wait()

	go func() {
		reader, err := sstore.Reader(streamID)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		readAll, err := ioutil.ReadAll(reader)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if string(readAll) != data {
			t.Fatalf("%s %s", string(readAll), data)
		}

		readAll, err = ioutil.ReadAll(reader)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if len(readAll) > 0 {
			t.Fatalf("Reader no data remain")
		}

		watcher := sstore.Watcher(streamID)
		defer watcher.Close()

		select {
		case pos := <-watcher.Watch():
			fmt.Println("end", pos)
		}

		readAll, err = ioutil.ReadAll(reader)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if string(readAll) != "hello world2" {
			t.Fatalf("%s ", string(readAll))
		}
	}()

	time.Sleep(time.Second)
	wg = sync.WaitGroup{}
	wg.Add(1)
	sstore.AsyncAppend(streamID, []byte("hello world2"), -1, func(offset int64, err error) {
		wg.Done()
		if err != nil {
			t.Fatalf("%+v", err)
		}
	})
	wg.Wait()
	if err := sstore.Close(); err != nil {
		t.Fatalf("%+v", err)
	}
}

func TestAsyncAppend(t *testing.T) {
	os.RemoveAll("data")
	defer os.RemoveAll("data")
	sstore, err := Open(DefaultOptions("data").
		WithMaxMStreamTableSize(128 * MB).
		WithMaxWalSize(512 * MB))
	if err != nil {
		t.Fatalf("%+v", err)
	}
	var data = make([]byte, 32)
	var wg sync.WaitGroup
	var count int64
	go func() {
		lCount := count
		for {
			tCount := count
			fmt.Println(int64(tCount-lCount) / 5)
			lCount = tCount
			time.Sleep(time.Second * 5)
		}
	}()
	for i := 0; i < 100; i++ {
		for i2 := 0; i2 < 100000; i2++ {
			wg.Add(1)
			sstore.AsyncAppend(int64(i2), data, -1, func(offset int64, err error) {
				if err != nil {
					t.Fatalf("%+v", err)
				}
				atomic.AddInt64(&count, 1)
				wg.Done()
			})
		}
	}
	wg.Wait()

	if err := sstore.GC(); err != nil {
		t.Fatalf("%+v", err)
	}

	if len(sstore.manifest.GetSegmentFiles()) == 5 {
		t.Fatalf("")
	}
}
