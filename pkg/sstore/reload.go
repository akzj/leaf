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
	block_queue "github.com/akzj/streamIO/pkg/block-queue"
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
		sStore.options.BlockSize)
	sStore.committer = committer

	sStore.committer.start()
	sStore.manifest.start()
	sStore.endWatchers.start()

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
	walFiles := manifest.getWalFiles()
	var cb = func(int64, error) {}
	for _, filename := range walFiles {
		journal, err := openJournal(filepath.Join(sStore.options.WalDir, filename))
		if err != nil {
			return err
		}
		//skip
		if walHeader, err := manifest.getWalHeader(filename); err == nil {
			if walHeader.Old && walHeader.To.Index <= sStore.version.Index {
				continue
			}
		}
		if err := journal.Read(func(e *writeRequest) error {
			if e.entry.Ver.Index <= sStore.version.Index {
				return nil //skip
			} else if e.entry.Ver.Index == sStore.version.Index+1 {
				e.cb = cb
				sStore.version = e.entry.Ver
				committer.queue.Push(e)
			} else {
				return errors.WithMessage(ErrWal,
					fmt.Sprintf("e.ID[%d] sStore.index+1[%d] %s",
						e.entry.Ver.Index, sStore.version.Index+1, filename))
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
		file, err := manifest.getNextWal()
		if err != nil {
			panic(err)
		}
		w, err = openJournal(file)
		if err != nil {
			return err
		}
		if err := manifest.appendWal(&pb.AppendWal{Filename: file}); err != nil {
			return err
		}
	}
	sStore.wWriter = newWWriter(w, sStore.entryQueue,
		sStore.committer.queue, sStore.manifest, sStore.options.MaxWalSize)
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
