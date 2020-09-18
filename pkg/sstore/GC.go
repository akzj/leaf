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
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"path/filepath"
)

// GC will delete journal,segment
// delete journal
// delete segment
func (sstore *SStore) clearJournal() error {
	journalFiles := sstore.manifest.GetJournalFiles()
	segmentFiles := sstore.manifest.GetSegmentFiles()
	if len(segmentFiles) == 0 {
		return nil
	}
	last := segmentFiles[len(segmentFiles)-1]
	segment := sstore.committer.getSegment(last)
	if segment == nil {
		return errors.Errorf("no find segment [%s]", last)
	}
	FromVersion := segment.FromVersion()
	segment.DecRef()
	for _, filename := range journalFiles {
		journalFile := filepath.Join(sstore.options.JournalDir, filename)
		header, err := sstore.manifest.GetJournalMeta(filename)
		if err != nil {
			continue
		}
		if header.Old && header.To.Index < FromVersion.Index {
			//first delete from manifest
			//and than delete from syncer
			log.Info("delete", journalFile)
			if err := sstore.manifest.DeleteJournal(&pb.DeleteJournal{Filename: filename}); err != nil {
				return err
			}
			if err := sstore.manifest.DelJournalMeta(&pb.DelJournalMeta{Filename: filename}); err != nil {
				return err
			}
			sstore.syncer.deleteJournal(journalFile)
		}
	}
	return nil
}

func (sstore *SStore) clearSegment() error {
	segmentFiles := sstore.manifest.GetSegmentFiles()
	if len(segmentFiles) <= sstore.options.MaxSegmentCount {
		return nil
	}
	var deleteFiles = segmentFiles[:len(segmentFiles)-sstore.options.MaxSegmentCount+1]
	for _, filename := range deleteFiles {
		segment := sstore.committer.getSegment(filename)
		if segment == nil {
			return errors.Errorf("no find segment[%s]", filename)
		}
		if err := sstore.manifest.DeleteSegment(&pb.DeleteSegment{Filename: filename}); err != nil {
			return err
		}
		if err := segment.deleteOnClose(true); err != nil {
			return err
		}
		if err := sstore.committer.deleteSegment(filename); err != nil {
			return err
		}
		segment.DecRef()
	}
	return nil
}
