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
	"github.com/akzj/streamIO/pkg/block-queue"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"strings"
)

func mkdir(dir string) error {
	f, err := os.Open(dir)
	if err == nil {
		_ = f.Close()
		return nil
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

//init segment,journal,index
func (sStore *SStore) init() error {
	for _, dir := range []string{
		sStore.options.JournalDir,
		sStore.options.ManifestDir,
		sStore.options.SegmentDir} {
		if err := mkdir(dir); err != nil {
			return err
		}
	}
	manifest, err := openManifest(sStore.options.ManifestDir,
		sStore.options.SegmentDir,
		sStore.options.JournalDir)
	if err != nil {
		return err
	}
	sStore.manifest = manifest

	mStreamTable := newMStreamTable(sStore.endMap, sStore.options.BlockSize, 128)
	commitQueue := block_queue.NewQueue(sStore.options.RequestQueueCap)
	committer := newCommitter(sStore.options,
		sStore.endWatchers,
		sStore.indexTable,
		sStore.endMap,
		mStreamTable,
		commitQueue,
		manifest,
		sStore.options.BlockSize,
		sStore)
	sStore.committer = committer
	sStore.syncer = newSyncer(sStore)

	sStore.committer.start()
	sStore.manifest.start()
	sStore.endWatchers.start()
	sStore.syncer.start()

	//rebuild segment index
	segmentFiles := manifest.getSegmentFiles()
	sortIntFilename(segmentFiles)
	for _, file := range segmentFiles {
		segment, err := openSegment(filepath.Join(sStore.options.SegmentDir, file))
		if err != nil {
			return err
		}
		for _, info := range segment.meta.OffSetInfos {
			sStore.endMap.set(info.StreamID, info.End, segment.meta.To)
		}
		if sStore.version == nil {
			sStore.version = segment.meta.To
		} else if segment.meta.From.Index <= sStore.version.Index {
			return errors.Errorf("segment meta LastEntryID[%d] error",
				segment.meta.From.Index)
		}
		sStore.version = segment.meta.To
		sStore.committer.appendSegment(file, segment)
	}
	if sStore.version == nil {
		sStore.version = &pb.Version{}
	}

	//replay entries in the journal
	journalFiles := manifest.getJournalFiles()
	var cb = func(int64, error) {}
	for _, filename := range journalFiles {
		journal, err := openJournal(filepath.Join(sStore.options.JournalDir, filename))
		if err != nil {
			return err
		}
		//skip
		if header, err := manifest.getJournalHeader(filename); err == nil {
			if header.Old && header.To.Index <= sStore.version.Index {
				continue
			}
		}
		if err := journal.Read(func(e *WriteRequest) error {
			if e.Entry.Ver.Index <= sStore.version.Index {
				return nil //skip
			} else if e.Entry.Ver.Index == sStore.version.Index+1 {
				e.cb = cb
				sStore.version = e.Entry.Ver
				committer.queue.Push(e)
			} else {
				return errors.WithMessage(ErrJournal,
					fmt.Sprintf("e.ID[%d] sStore.index+1[%d] %s",
						e.Entry.Ver.Index, sStore.version.Index+1, filename))
			}
			return nil
		}); err != nil {
			_ = journal.Close()
			return err
		}
		if err := journal.Close(); err != nil {
			return err
		}
	}

	//create journal writer
	var journal *journal
	fmt.Println(journalFiles)
	if len(journalFiles) > 0 {
		journal, err = openJournal(filepath.Join(sStore.options.JournalDir, journalFiles[len(journalFiles)-1]))
		if err != nil {
			return err
		}
		if err := journal.RebuildIndex(); err != nil {
			return err
		}
		fmt.Println("journal.RebuildIndex() done",journal.meta)
	} else {
		file, err := manifest.geNextJournal()
		if err != nil {
			panic(err)
		}
		journal, err = openJournal(file)
		if err != nil {
			return err
		}
		if err := manifest.AppendJournal(&pb.AppendJournal{Filename: file}); err != nil {
			return err
		}
	}

	sStore.syncer.appendJournal(journal)

	//rebuild journal index
	for _, filename := range manifest.getJournalFiles() {
		journalFile := filepath.Join(sStore.options.JournalDir, filename)
		if journalFile == journal.filename {
			continue
		}

		if journal, err := openJournal(journalFile); err != nil {
			return err
		} else {
			if err := journal.RebuildIndex(); err != nil {
				return err
			}
			_ = journal.Close()
			sStore.syncer.appendJournal(journal)
		}
	}

	sStore.journalWriter = newJournalWriter(journal, sStore.entryQueue,
		sStore.committer.queue, sStore.syncer, sStore.manifest, sStore.options.MaxJournalSize)
	sStore.journalWriter.start()

	//clear dead journal
	journalFiles = manifest.getJournalFiles()
	allJournalFiles, err := listDir(sStore.options.JournalDir, manifestExt)
	if err != nil {
		return err
	}
	for _, filename := range diffStrings(allJournalFiles, journalFiles) {
		if err := os.Remove(filepath.Join(sStore.options.JournalDir, filename)); err != nil {
			return errors.WithStack(err)
		}
		fmt.Println("delete filename " + filename)
	}

	//clear dead segment manifest
	segmentFiles = manifest.getSegmentFiles()
	segmentFileAll, err := listDir(sStore.options.SegmentDir, segmentExt)
	if err != nil {
		return err
	}
	for _, filename := range diffStrings(segmentFileAll, segmentFiles) {
		if err := os.Remove(filepath.Join(sStore.options.SegmentDir, filename)); err != nil {
			return errors.WithStack(err)
		}
		fmt.Println("delete filename " + filename)
	}
	return nil
}

func listDir(dir string, ext string) ([]string, error) {
	var files []string
	return files, filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if info.IsDir() {
			return nil
		}
		if ext != "" && strings.HasSuffix(info.Name(), ext) {
			files = append(files, info.Name())
		}
		return nil
	})
}
func diffStrings(all []string, sub []string) []string {
	var diff []string
Loop:
	for _, it := range all {
		for _, ij := range sub {
			if it == ij {
				continue Loop
			}
		}
		diff = append(diff, it)
	}
	return diff
}
