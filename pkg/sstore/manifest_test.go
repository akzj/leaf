package sstore

import (
	"fmt"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func prepareManifest(t *testing.T) {
	assert.NoError(t, os.MkdirAll("data/manifest/", 0777))
}

func clearManifest(t *testing.T) {
	assert.NoError(t, os.RemoveAll("data/manifest"))
}

func TestManifest(t *testing.T) {
	clearManifest(t)
	prepareManifest(t)
	defer clearManifest(t)
	var manifest *Manifest
	var err error
	t.Run("OpenManifest", func(t *testing.T) {
		manifest, err = OpenManifest("data/manifest", "data/segments", "data/journals")
		assert.NoError(t, err)
	})

	var journalCount = 10000
	t.Run("AppendJournal", func(t *testing.T) {
		for i := 0; i < journalCount; i++ {
			assert.NoError(t, manifest.AppendJournal(&pb.AppendJournal{
				Filename: fmt.Sprintf("%d.log", i),
			}))
		}
	})

	var segmentCount = 10000
	t.Run("AppendSegment", func(t *testing.T) {
		for i := 0; i < segmentCount; i++ {
			assert.NoError(t, manifest.AppendSegment(&pb.AppendSegment{
				Filename: fmt.Sprintf("%d.seg", i),
			}))
		}
	})

	var journalHeaderCount = 10000
	t.Run("SetJournalMeta", func(t *testing.T) {
		for i := 0; i < journalHeaderCount; i++ {
			assert.NoError(t, manifest.SetJournalMeta(&pb.JournalMeta{
				Old:      true,
				Filename: fmt.Sprintf("%d.log", i),
				From: &pb.Version{
					Term:  0,
					Index: int64(i),
				},
				To: &pb.Version{
					Term:  0,
					Index: int64(i),
				},
			}))
		}
	})

	t.Run("test_compctionLog", func(t *testing.T) {
		manifest.maxJournalSize = 0
		assert.NoError(t, manifest.compactionLog())
	})

	t.Run("Close", func(t *testing.T) {
		assert.NoError(t, manifest.Close())
	})

	t.Run("OpenManifest_exist", func(t *testing.T) {
		manifest, err = OpenManifest("data/manifest", "data/segments", "data/journals")
		assert.NoError(t, err)
	})

	t.Run("GetJournalFiles", func(t *testing.T) {
		var count int
		for _, filename := range manifest.GetJournalFiles() {
			assert.Equal(t, filename, fmt.Sprintf("%d.log", count))
			count++
		}
		assert.Equal(t, count, journalCount)
	})

	t.Run("GetJournalFiles", func(t *testing.T) {
		var count int
		for _, filename := range manifest.GetSegmentFiles() {
			assert.Equal(t, filename, fmt.Sprintf("%d.seg", count))
			count++
		}
		assert.Equal(t, count, segmentCount)
	})

	t.Run("GetJournalFiles", func(t *testing.T) {
		for i := 0; i < journalCount; i++ {
			filename := fmt.Sprintf("%d.log", i)
			meta, err := manifest.GetJournalMeta(filename)
			assert.NoError(t, err)
			assert.Equal(t, meta.Filename, filename)
		}
	})

	//delete

	t.Run("DeleteJournal", func(t *testing.T) {
		for _, filename := range manifest.GetJournalFiles() {
			assert.NoError(t, manifest.DeleteJournal(&pb.DeleteJournal{Filename: filename}))
		}
	})

	t.Run("DeleteSegment", func(t *testing.T) {
		for _, filename := range manifest.GetSegmentFiles() {
			assert.NoError(t, manifest.DeleteSegment(&pb.DeleteSegment{Filename: filename}))
		}
	})

	t.Run("DelJournalMeta", func(t *testing.T) {
		for i := 0; i < journalCount; i++ {
			filename := fmt.Sprintf("%d.log", i)
			assert.NoError(t, manifest.DelJournalMeta(&pb.DelJournalMeta{Filename: filename}))
		}
	})

	assert.Equal(t, 0, len(manifest.GetJournalFiles()))
	assert.Equal(t, 0, len(manifest.GetSegmentFiles()))
	assert.Equal(t, 0, len(manifest.GetJournalMetas()))

	t.Run("Close", func(t *testing.T) {
		assert.NoError(t, manifest.Close())
	})
}
