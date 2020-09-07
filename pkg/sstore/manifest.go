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
	"encoding/json"
	"github.com/pkg/errors"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type appendWal struct {
	Filename string `json:"filename"`
}
type deleteWal struct {
	Filename string `json:"filename"`
}

type appendSegment struct {
	Filename string `json:"filename"`
}

type deleteSegment struct {
	Filename string `json:"filename"`
}

type delWalHeader struct {
	Filename string `json:"filename"`
}

type manifest struct {
	l              sync.RWMutex
	maxJournalSize int64
	journal        *journal
	segmentDir     string
	manifestDir    string
	walDir         string
	segmentIndex   int64
	walIndex       int64
	filesIndex     int64
	inRecovery     bool

	EntryID      int64                  `json:"entry_id"`
	Segments     []string               `json:"segments"`
	Journals     []string               `json:"journals"`
	WalHeaderMap map[string]JournalMeta `json:"wal_header_map"`

	notifySnap chan interface{}
	c          chan interface{}
	s          chan interface{}
}

const (
	appendSegmentType    = iota //"appendSegment" //append segment
	deleteSegmentType           //= "deleteSegment"
	appendWalType               //= "appendWal"
	deleteWalType               //= "deleteWal"
	setWalHeaderType            //= "setWalHeader" //set journal meta
	delWalHeaderType            //= "delWalHeader" //set journal meta
	manifestSnapshotType        //= "filesSnapshot"

	segmentExt            = ".seg"
	manifestExt           = ".log"
	manifestJournalExt    = ".mlog"
	manifestJournalExtTmp = ".mlog.tmp"
)

func openManifest(manifestDir string, segmentDir string, walDir string) (*manifest, error) {
	files := &manifest{
		maxJournalSize: 128 * MB,
		journal:        nil,
		l:              sync.RWMutex{},
		segmentDir:     segmentDir,
		manifestDir:    manifestDir,
		walDir:         walDir,
		segmentIndex:   0,
		walIndex:       0,
		filesIndex:     0,
		inRecovery:     false,
		EntryID:        0,
		Segments:       make([]string, 0, 128),
		Journals:       make([]string, 0, 128),
		notifySnap:     make(chan interface{}, 1),
		c:              make(chan interface{}, 1),
		s:              make(chan interface{}, 1),
		WalHeaderMap:   make(map[string]JournalMeta),
	}
	if err := files.reload(); err != nil {
		return nil, err
	}
	return files, nil
}

func copyStrings(strings []string) []string {
	return append(make([]string, 0, len(strings)), strings...)
}

func (f *manifest) getSegmentFiles() []string {
	f.l.RLock()
	defer f.l.RUnlock()
	return copyStrings(f.Segments)
}

func (f *manifest) getWalFiles() []string {
	f.l.RLock()
	defer f.l.RUnlock()
	return copyStrings(f.Journals)
}

func (f *manifest) reload() error {
	f.inRecovery = true
	defer func() {
		f.inRecovery = false
	}()
	var logFiles []string
	err := filepath.Walk(f.manifestDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if strings.HasSuffix(info.Name(), manifestJournalExtTmp) {
			_ = os.Remove(path)
		}
		if strings.HasSuffix(info.Name(), manifestJournalExt) {
			logFiles = append(logFiles, path)
		}
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}

	sortIntFilename(logFiles)
	if len(logFiles) == 0 {
		f.filesIndex = 1
		f.journal, err = openJournal(filepath.Join(f.manifestDir, "1"+manifestJournalExt))
	} else {
		f.journal, err = openJournal(logFiles[len(logFiles)-1])
		if err != nil {
			return err
		}
	}
	err = f.journal.Read(func(e *entry) error {
		f.EntryID = e.ID
		switch e.StreamID {
		case appendSegmentType:
			var appendS appendSegment
			if err := json.Unmarshal(e.data, &appendS); err != nil {
				return errors.WithStack(err)
			}
			return f.appendSegment(appendS)
		case deleteSegmentType:
			var deleteS deleteSegment
			if err := json.Unmarshal(e.data, &deleteS); err != nil {
				return errors.WithStack(err)
			}
			return f.deleteSegment(deleteS)
		case appendWalType:
			var appendW appendWal
			if err := json.Unmarshal(e.data, &appendW); err != nil {
				return errors.WithStack(err)
			}
			return f.appendWal(appendW)
		case deleteWalType:
			var deleteW deleteWal
			if err := json.Unmarshal(e.data, &deleteW); err != nil {
				return errors.WithStack(err)
			}
			return f.deleteWal(deleteW)
		case manifestSnapshotType:
			if err := json.Unmarshal(e.data, f); err != nil {
				return errors.WithStack(err)
			}
		case setWalHeaderType:
			var header JournalMeta
			if err := json.Unmarshal(e.data, &header); err != nil {
				return errors.WithStack(err)
			}
			return f.setWalHeader(header)
		default:
			log.Fatalf("unknown type %d", e.StreamID)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(f.Segments) > 0 {
		sortIntFilename(f.Segments)
		f.segmentIndex, err = parseFilenameIndex(f.Segments[len(f.Segments)-1])
		if err != nil {
			return errors.WithStack(err)
		}
	}
	if len(f.Journals) > 0 {
		sortIntFilename(f.Journals)
		f.walIndex, err = parseFilenameIndex(f.Journals[len(f.Journals)-1])
		if err != nil {
			return errors.WithStack(err)
		}
	}
	if len(logFiles) > 0 {
		for _, filename := range logFiles[:len(logFiles)-1] {
			if err := os.Remove(filename); err != nil {
				return errors.WithStack(err)
			}
		}
		f.filesIndex, err = parseFilenameIndex(logFiles[len(logFiles)-1])
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (f *manifest) getNextSegment() string {
	atomic.AddInt64(&f.segmentIndex, 1)
	return filepath.Join(f.segmentDir, strconv.FormatInt(f.segmentIndex, 10)+segmentExt)
}

func (f *manifest) makeSnapshot() {
	f.l.Lock()
	defer f.l.Unlock()
	if f.journal.Size() < f.maxJournalSize {
		return
	}
	f.filesIndex++
	f.EntryID++
	tmpJournal := strconv.FormatInt(f.filesIndex, 10) + manifestJournalExtTmp
	tmpJournal = filepath.Join(f.manifestDir, tmpJournal)
	journal, err := openJournal(tmpJournal)
	if err != nil {
		log.Fatal(err)
	}
	data, err := json.Marshal(f)
	if err != nil {
		log.Fatal(err)
	}
	if err := journal.Write(&entry{
		ID:   f.EntryID,
		StreamID: manifestSnapshotType,
		data: data,
		cb:   nil,
	}); err != nil {
		log.Fatal(err)
	}
	if err := journal.Flush(); err != nil {
		log.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		log.Fatal(err)
	}
	filename := strings.ReplaceAll(tmpJournal, manifestJournalExtTmp, manifestJournalExt)
	if err := os.Rename(tmpJournal, filename); err != nil {
		log.Fatal(err)
	}
	if err := f.journal.Flush(); err != nil {
		log.Fatal(err)
	}
	if err := f.journal.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.Remove(f.journal.Filename()); err != nil {
		log.Fatal(err)
	}
	f.journal, err = openJournal(filename)
	if err != nil {
		log.Fatal(err)
	}
}

func (f *manifest) appendWal(appendW appendWal) error {
	f.l.Lock()
	defer f.l.Unlock()
	filename := filepath.Base(appendW.Filename)
	for _, file := range f.Journals {
		if file == filename {
			return errors.Errorf("journal filename repeated")
		}
	}
	f.Journals = append(f.Journals, filename)
	sortIntFilename(f.Journals)
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(appendW)
	return f.writeEntry(appendWalType, data)
}

func (f *manifest) deleteWal(deleteW deleteWal) error {
	f.l.Lock()
	defer f.l.Unlock()
	filename := filepath.Base(deleteW.Filename)
	var find = false
	for index, file := range f.Journals {
		if file == filename {
			copy(f.Journals[index:], f.Journals[index+1:])
			f.Journals = f.Journals[:len(f.Journals)-1]
			find = true
			break
		}
	}
	if find == false {
		return errors.Errorf("no find journal file [%s]", filename)
	}
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(deleteW)
	return f.writeEntry(deleteWalType, data)
}

func (f *manifest) appendSegment(appendS appendSegment) error {
	f.l.Lock()
	defer f.l.Unlock()
	filename := filepath.Base(appendS.Filename)
	for _, file := range f.Segments {
		if file == filename {
			return errors.Errorf("segment filename repeated")
		}
	}
	f.Segments = append(f.Segments, filename)
	sortIntFilename(f.Segments)
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(appendS)
	return f.writeEntry(appendSegmentType, data)
}

func (f *manifest) setWalHeader(header JournalMeta) error {
	f.l.Lock()
	defer f.l.Unlock()
	f.WalHeaderMap[filepath.Base(header.Filename)] = header
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(header)
	return f.writeEntry(setWalHeaderType, data)
}

func (f *manifest) getWalHeader(filename string) (JournalMeta, error) {
	f.l.Lock()
	defer f.l.Unlock()
	header, ok := f.WalHeaderMap[filename]
	if !ok {
		return header, errors.Errorf("no find meta [%s]", filename)
	}
	return header, nil
}

func (f *manifest) delWalHeader(header delWalHeader) error {
	f.l.Lock()
	defer f.l.Unlock()
	_, ok := f.WalHeaderMap[filepath.Base(header.Filename)]
	if ok == false {
		return errors.Errorf("no find journal [%s]", header.Filename)
	}
	delete(f.WalHeaderMap, filepath.Base(header.Filename))
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(header)
	return f.writeEntry(delWalHeaderType, data)
}

func (f *manifest) deleteSegment(deleteS deleteSegment) error {
	f.l.Lock()
	defer f.l.Unlock()
	filename := filepath.Base(deleteS.Filename)
	var find = false
	for index, file := range f.Segments {
		if file == filename {
			copy(f.Segments[index:], f.Segments[index+1:])
			f.Segments = f.Segments[:len(f.Segments)-1]
			find = true
			break
		}
	}
	if find == false {
		return errors.Errorf("no find segment [%s]", filename)
	}
	if f.inRecovery {
		return nil
	}
	data, _ := json.Marshal(deleteS)
	return f.writeEntry(deleteSegmentType, data)
}

func (f *manifest) writeEntry(typ int64, data []byte, ) error {
	f.EntryID++
	if err := f.journal.Write(&entry{
		ID:   f.EntryID,
		StreamID: typ,
		data: data,
	}); err != nil {
		return err
	}
	if err := f.journal.Flush(); err != nil {
		return err
	}
	if f.journal.Size() > f.maxJournalSize {
		f.notifySnapshot()
	}
	return nil
}

func (f *manifest) notifySnapshot() {
	select {
	case f.notifySnap <- struct{}{}:
	default:
	}
}

func (f *manifest) close() {
	f.l.Lock()
	defer f.l.Unlock()
	_ = f.journal.Close()
	close(f.c)
	<-f.s
}

func (f *manifest) start() {
	go func() {
		for {
			select {
			case <-f.c:
				close(f.s)
				return
			case <-f.notifySnap:
				f.makeSnapshot()
			}
		}
	}()
}

func (f *manifest) getNextWal() string {
	f.l.Lock()
	f.l.Unlock()
	f.walIndex++
	return filepath.Join(f.walDir, strconv.FormatInt(f.walIndex, 10)+manifestExt)
}

func parseFilenameIndex(filename string) (int64, error) {
	filename = filepath.Base(filename)
	token := strings.SplitN(filename, ".", 2)[0]
	return strconv.ParseInt(token, 10, 64)
}

func sortIntFilename(intFiles []string) {
	sort.Slice(intFiles, func(i, j int) bool {
		iLen := len(intFiles[i])
		jLen := len(intFiles[j])
		if iLen != jLen {
			return iLen < jLen
		}
		return intFiles[i] < intFiles[j]
	})
}
