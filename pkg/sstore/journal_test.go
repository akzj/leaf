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
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"strconv"
	"testing"
)

func prepareJournalDir(t *testing.T) {
	assert.NoError(t, os.MkdirAll("data/journal/", 0777))
}

func clearJournalDir(t *testing.T) {
	assert.NoError(t, os.RemoveAll("data/journal"))
}

func TestOpenJournal(t *testing.T) {
	prepareJournalDir(t)
	defer clearJournalDir(t)
	journal, err := OpenJournal("data/journal/1.log")
	assert.NoError(t, err)
	assert.NoError(t, journal.Close())
}
func TestJournal(t *testing.T) {
	prepareJournalDir(t)
	defer clearJournalDir(t)

	var journal *journal
	var err error
	t.Run("test_OpenJournal", func(t *testing.T) {
		journal, err = OpenJournal("data/journal/1.log")
		assert.NoError(t, err)
	})

	var count = 10000
	t.Run("test_journal_write", func(t *testing.T) {
		for i := 0; i < count; i++ {
			assert.NoError(t, journal.BatchWrite(&pb.BatchEntry{Entries: []*pb.Entry{
				&pb.Entry{
					StreamID: 0,
					Offset:   0,
					Data:     []byte(strconv.Itoa(i)),
				},
			}, Ver: &pb.Version{Index: int64(i)},
			}))
			assert.NoError(t, journal.Flush())
		}
	})

	t.Run("test_journal_mmap_read", func(t *testing.T) {
		jMap := journal.GetJournalMMap()
		assert.NotNil(t, jMap)

		reader := bytes.NewReader(jMap.data)

		for i := 0; i < count; i++ {
			index, err := journal.Index().find(int64(i))
			assert.NoError(t, err)
			_, err = reader.Seek(index.Offset, io.SeekStart)
			assert.NoError(t, err)
			entry, err := DecodeBatchEntry(reader)
			assert.NoError(t, err)
			assert.Equal(t, int64(i), entry.Ver.Index)
		}
	})

	t.Run("test_journal_close", func(t *testing.T) {
		assert.NoError(t, journal.Close())
	})

	t.Run("test_Journal_open_exist_log", func(t *testing.T) {
		//open exist journal
		journal, err = OpenJournal("data/journal/1.log")
		assert.NoError(t, err)

	})

	t.Run("test_Journal_range_entry", func(t *testing.T) {
		var index int64
		assert.NoError(t, journal.Range(func(entry *pb.BatchEntry) error {
			assert.Equal(t, entry.Ver.Index, index)
			index++
			return nil
		}))
		assert.Equal(t, index, int64(count))
	})

	t.Run("test_journal_close", func(t *testing.T) {
		assert.NoError(t, journal.Close())
	})

	t.Run("test_journal_mmap_release", func(t *testing.T) {
		assert.Nil(t, journal.GetJournalMMap())
		assert.True(t, journal.JournalMMap.Count() < 0)
		assert.Nil(t, journal.JournalMMap.data)
	})
}
