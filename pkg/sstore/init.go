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
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
)

func mkdir(dir string) error {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (store *Store) gogo(f func()) {
	store.wg.Add(1)
	go func() {
		defer store.wg.Done()
		f()
	}()
}

//init segment,journal,sections
func (store *Store) init() error {
	for _, dir := range []string{
		store.options.JournalDir,
		store.options.ManifestDir,
		store.options.SegmentDir} {
		if err := mkdir(dir); err != nil {
			return err
		}
	}
	manifest, err := OpenManifest(store.options.ManifestDir,
		store.options.SegmentDir,
		store.options.JournalDir)
	if err != nil {
		return err
	}
	store.manifest = manifest

	commitQueue := block_queue.NewQueueWithContext(store.ctx, store.options.RequestQueueCap)
	callbackQueue := block_queue.NewQueueWithContext(store.ctx, store.options.RequestQueueCap)
	flushSegmentQueue := block_queue.NewQueueWithContext(store.ctx, store.options.SegmentFlushQueue)
	notifyQueue := block_queue.NewQueueWithContext(store.ctx, store.options.RequestQueueCap)
	journalQueue := block_queue.NewQueueWithContext(store.ctx, store.options.RequestQueueCap)

	store.streamWatcher = newStreamWatcher(notifyQueue)
	store.entryQueue = journalQueue

	store.sectionsTableUpdater = &sectionsTableUpdater{
		callbackQueue: callbackQueue,
		notifyQueue:   notifyQueue,
		queue:         block_queue.NewQueueWithContext(store.ctx, 128),
		sectionsTable: store.sectionsTable,
	}
	store.callbackWorker = newCallbackWorker(callbackQueue)

	store.committer = newCommitter(store,
		commitQueue,
		flushSegmentQueue,
		store.sectionsTableUpdater.queue)
	store.syncer = newSyncer(store)
	store.flusher = newSegmentFlusher(store.options.SegmentDir, flushSegmentQueue)

	store.gogo(store.callbackWorker.callbackLoop)
	store.gogo(store.flusher.flushLoop)
	store.gogo(store.committer.processLoop)
	store.gogo(store.streamWatcher.notifyLoop)
	store.gogo(store.syncer.notifySubscriberLoop)
	store.gogo(store.sectionsTableUpdater.updateLoop)

	//rebuild segment index
	segmentFiles := manifest.GetSegmentFiles()
	sortFilename(segmentFiles)
	for _, file := range segmentFiles {
		segment, err := openSegment(filepath.Join(store.options.SegmentDir, file))
		if err != nil {
			return err
		}
		for _, info := range segment.meta.SectionOffsets {
			store.endMap.set(info.StreamID, info.End, segment.meta.To)
		}
		if store.version == nil {
			store.version = segment.meta.To
		} else if segment.meta.From.Index <= store.version.Index {
			return errors.Errorf("segment meta LastEntryID[%d] error",
				segment.meta.From.Index)
		}
		store.version = segment.meta.To
		store.appendSegment(file, segment)
		log.Infof("segment index [%d,%d]",
			segment.meta.From.Index, segment.meta.To.Index)
	}
	if store.version == nil {
		store.version = &pb.Version{}
	}

	//replay entries in the journal
	var leastJournal *journal
	journalFiles := manifest.GetJournalFiles()
	for index, filename := range journalFiles {
		least := filename == journalFiles[index]
		journal, err := OpenJournal(filepath.Join(store.options.JournalDir, filename))
		if err != nil {
			return err
		}
		header, err := manifest.GetJournalMeta(filename)
		if err != nil {
			if !least {
				return err
			}
		} else {
			if header.Old && header.To.Index < store.version.Index {
				_ = journal.Close()
				continue
			}
		}
		if err := journal.Range(func(entry *pb.BatchEntry) error {
			if entry.Ver.Index <= store.version.Index {
				return nil
			} else if entry.Ver.Index == store.version.Index+1 {
				store.version = entry.Ver
				if err := store.committer.queue.Push(&BatchAppend{
					Entry: entry,
					cb:    func(err error) {},
				}); err != nil {
					log.Fatal(err)
				}
			} else {
				return errors.WithMessage(ErrJournal,
					fmt.Sprintf("entry.ID[%d] store.index+1[%d] %s",
						entry.Ver.Index, store.version.Index+1, filename))
			}
			return nil
		}); err != nil {
			_ = journal.Close()
			return err
		}
		log.Infof("journal index [%d,%d]", journal.GetMeta().From.Index, journal.GetMeta().To.Index)
		store.syncer.appendJournal(journal)
		if least == false {
			if err := journal.Close(); err != nil {
				return err
			}
		} else {
			leastJournal = journal
		}
	}

	//create journal writer
	if leastJournal == nil {
		file, err := manifest.NextJournal()
		if err != nil {
			panic(err)
		}
		leastJournal, err = OpenJournal(file)
		if err != nil {
			return err
		}
		if err := manifest.AppendJournal(&pb.AppendJournal{Filename: file}); err != nil {
			return err
		}
		store.syncer.appendJournal(leastJournal)
	}

	store.journalWriter = newJournalWriter(leastJournal,
		journalQueue,
		store.committer.queue,
		store.syncer.journalAppendCh,
		store.syncer,
		store.manifest,
		store.options.MaxJournalSize)

	store.gogo(store.journalWriter.writeLoop)

	//clear dead journal
	journalFiles = manifest.GetJournalFiles()
	allJournalFiles, err := listDir(store.options.JournalDir, manifestExt)
	if err != nil {
		return err
	}
	for _, filename := range diffStrings(allJournalFiles, journalFiles) {
		if err := os.Remove(filepath.Join(store.options.JournalDir, filename)); err != nil {
			return errors.WithStack(err)
		}
		fmt.Println("delete filename " + filename)
	}

	//clear dead segment manifest
	segmentFiles = manifest.GetSegmentFiles()
	segmentFileAll, err := listDir(store.options.SegmentDir, segmentExt)
	if err != nil {
		return err
	}
	for _, filename := range diffStrings(segmentFileAll, segmentFiles) {
		if err := os.Remove(filepath.Join(store.options.SegmentDir, filename)); err != nil {
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
