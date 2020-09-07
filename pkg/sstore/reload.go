// Copyright 2020-2026 The sstore Authors
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

//reload segment,journal,index
func reload(sStore *SStore) error {
	for _, dir := range []string{
		sStore.options.WalDir,
		sStore.options.ManifestDir,
		sStore.options.SegmentDir} {
		if err := mkdir(dir); err != nil {
			return err
		}
	}
	manifest, err := openManifest(sStore.options.ManifestDir,
		sStore.options.SegmentDir,
		sStore.options.WalDir)
	if err != nil {
		return err
	}
	sStore.files = manifest

	mStreamTable := newMStreamTable(sStore.endMap, sStore.options.BlockSize, 128)
	commitQueue := newEntryQueue(sStore.options.EntryQueueCap)
	committer := newCommitter(sStore.options,
		sStore.endWatchers,
		sStore.indexTable,
		sStore.segments,
		sStore.endMap,
		mStreamTable,
		commitQueue,
		manifest,
		sStore.options.BlockSize)
	sStore.committer = committer

	sStore.committer.start()
	sStore.files.start()
	sStore.endWatchers.start()

	//rebuild segment index
	segmentFiles := manifest.getSegmentFiles()
	for _, file := range segmentFiles {
		segment, err := openSegment(filepath.Join(sStore.options.SegmentDir, file))
		if err != nil {
			return err
		}
		for _, info := range segment.meta.OffSetInfos {
			sStore.endMap.set(info.StreamID, info.End, segment.meta.Ver)
		}
		if segment.meta.LastEntryID <= sStore.entryID {
			return errors.Errorf("segment meta LastEntryID[%d] error",
				segment.meta.LastEntryID)
		}
		sStore.entryID = segment.meta.LastEntryID
		sStore.segments[file] = segment
		if err := sStore.indexTable.update1(segment); err != nil {
			return err
		}
	}

	//replay entries in the journal
	walFiles := manifest.getWalFiles()
	var cb = func(int64, error) {}
	for _, filename := range walFiles {
		journal, err := openJournal(filepath.Join(sStore.options.WalDir, filename))
		if err != nil {
			return err
		}
		//skip
		if walHeader, err := manifest.getWalHeader(filename); err == nil {
			if walHeader.Old && walHeader.LastEntryID <= sStore.entryID {
				continue
			}
		}
		if err := journal.Read(func(e *entry) error {
			if e.ID <= sStore.entryID {
				return nil //skip
			} else if e.ID == sStore.entryID+1 {
				e.cb = cb
				sStore.entryID++
				committer.queue.put(e)
			} else {
				return errors.WithMessage(ErrWal,
					fmt.Sprintf("e.ID[%d] sStore.entryID+1[%d] %s", e.ID, sStore.entryID+1, filename))
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
	var w *journal
	if len(walFiles) > 0 {
		w, err = openJournal(filepath.Join(sStore.options.WalDir, walFiles[len(walFiles)-1]))
		if err != nil {
			return err
		}
		if err := w.SeekEnd(); err != nil {
			return errors.WithStack(err)
		}
	} else {
		file := manifest.getNextWal()
		w, err = openJournal(file)
		if err != nil {
			return err
		}
		if err := manifest.appendWal(appendWal{Filename: file}); err != nil {
			return err
		}
	}
	sStore.wWriter = newWWriter(w, sStore.entryQueue,
		sStore.committer.queue, sStore.files, sStore.options.MaxWalSize)
	sStore.wWriter.start()

	//clear dead journal
	walFiles = manifest.getWalFiles()
	walFileAll, err := listDir(sStore.options.WalDir, manifestExt)
	if err != nil {
		return err
	}
	for _, filename := range diffStrings(walFileAll, walFiles) {
		if err := os.Remove(filepath.Join(sStore.options.WalDir, filename)); err != nil {
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
func diffStrings(first []string, second []string) []string {
	var diff []string
Loop:
	for _, it := range first {
		for _, ij := range second {
			if it == ij {
				continue Loop
			}
		}
		diff = append(diff, it)
	}
	return diff
}
