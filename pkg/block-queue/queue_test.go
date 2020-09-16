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

package block_queue

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewQueue(t *testing.T) {
	queue := NewQueue(128)

	queue.Push(1)
	if queue.Pop().(int) != 1 {
		t.Fatalf("pop error")
	}
	items := []interface{}{1, 2, 3, 4, 5, 6, 7}
	queue.PushMany(items)
	if all := queue.PopAll(nil); reflect.DeepEqual(all, items) == false {
		t.Fatalf("PopAll error %+v", all)
	}
	queue.Push(nil)
	if queue.Pop() != nil {
		t.Fatalf("Push nil failed")
	}
}

func TestMultiGoroutine(t *testing.T) {
	queue := NewQueue(128)
	go func() {
		for {
			queue.Push(1)
		}
	}()
	var pos int64
	go func() {
		for {
			var buf []interface{}
			for {
				buf = queue.PopAll(buf)
				atomic.AddInt64(&pos, int64(len(buf)))
			}
		}
	}()

	var last = pos
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		p := atomic.LoadInt64(&pos)
		fmt.Println("items/second", p-last)
		last = p
	}
}

func BenchmarkPushPop(b *testing.B) {
	queue := NewQueue(128)
	begin := time.Now()
	var counts int64
	go func() {
		for {
			queue.Pop()
			counts++
		}
	}()
	b.ReportAllocs()
	b.N = 50000000
	for i := 0; i < b.N; i++ {
		queue.Push(i)
	}
	fmt.Println("items/second", int64(float64(counts)/time.Now().Sub(begin).Seconds()))
}

func BenchmarkPushPopContextQueue(b *testing.B) {
	queue := NewQueueWithContext(context.Background(), 128)
	begin := time.Now()
	var counts int64
	go func() {
		for {
			queue.Pop()
			counts++
		}
	}()
	b.ReportAllocs()
	b.N = 50000000
	for i := 0; i < b.N; i++ {
		queue.Push(i)
	}
	fmt.Println("items/second", int64(float64(counts)/time.Now().Sub(begin).Seconds()))
}

func BenchmarkPushPopAll(b *testing.B) {
	queue := NewQueue(128)
	begin := time.Now()
	var counts int64
	go func() {
		var buf []interface{}
		for {
			buf = queue.PopAll(buf)
			counts += int64(len(buf))
		}
	}()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		queue.Push(i)
	}
	fmt.Println("items/second", int64(float64(counts)/time.Now().Sub(begin).Seconds()))
}

func BenchmarkQueue_PushManyPopAll(b *testing.B) {
	queue := NewQueue(128)
	begin := time.Now()
	var counts int64
	go func() {
		var buf []interface{}
		for {
			buf = queue.PopAll(buf)
			counts += int64(len(buf))
		}
	}()
	var items = []interface{}{1, 2, 3, 4, 5, 6, 7}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		queue.PushMany(items)
	}
	fmt.Println("items/second", int64(float64(counts)/time.Now().Sub(begin).Seconds()))
}
