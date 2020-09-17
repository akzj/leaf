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
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	"path/filepath"
)

// GC will delete journal,segment
// delete journal
// delete segment
func (sstore *SStore) clearJournal() error {
	journalFiles := sstore.manifest.getJournalFiles()
	segmentFiles := sstore.manifest.getSegmentFiles()
	if len(segmentFiles) == 0 {
		return nil
	}
	last := segmentFiles[len(segmentFiles)-1]
	segment := sstore.committer.getSegment(last)
	if segment == nil {
		return errors.Errorf("no find segment [%s]", last)
	}
	FromVersion := segment.FromVersion()
	segment.refDec()
	for _, filename := range journalFiles {
		journalFile := filepath.Join(sstore.options.JournalDir, filename)
		header, err := sstore.manifest.getJournalHeader(filename)
		if err != nil {
			continue
		}
		if header.Old && header.To.Index < FromVersion.Index {
			//first delete from manifest
			//and than delete from syncer
			fmt.Println("delete", journalFile)
			if err := sstore.manifest.deleteJournal(&pb.DeleteJournal{Filename: filename}); err != nil {
				return err
			}
			if err := sstore.manifest.delWalHeader(&pb.DelJournalHeader{Filename: filename}); err != nil {
				return err
			}
			sstore.syncer.deleteJournal(journalFile)
		}
	}
	return nil
}

func (sstore *SStore) clearSegment() error {
	segmentFiles := sstore.manifest.getSegmentFiles()
	if len(segmentFiles) <= sstore.options.MaxSegmentCount {
		return nil
	}
	var deleteFiles = segmentFiles[:len(segmentFiles)-sstore.options.MaxSegmentCount+1]
	for _, filename := range deleteFiles {
		segment := sstore.committer.getSegment(filename)
		if segment == nil {
			return errors.Errorf("no find segment[%s]", filename)
		}
		if err := sstore.manifest.deleteSegment(&pb.DeleteSegment{Filename: filename}); err != nil {
			return err
		}
		if err := segment.deleteOnClose(true); err != nil {
			return err
		}
		if err := sstore.committer.deleteSegment(filename); err != nil {
			return err
		}
		segment.refDec()
	}
	return nil
}
