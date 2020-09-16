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
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type manifest struct {
	*pb.ManifestSnapshot
	l                   sync.RWMutex
	maxJournalSize      int64
	journal             *journal
	segmentDir          string
	manifestDir         string
	journalDir          string
	inRecovery          bool
	notifyCompactionLog chan interface{}
	c                   chan interface{}
	s                   chan interface{}
}

const (
	appendSegmentType    = iota //"appendSegment" //append segment
	deleteSegmentType           //= "deleteSegment"
	appendJournalType           //= "appendJournalType"
	deleteJournalType           //= "deleteJournal"
	setJournalHeaderType        //= "setJournalHeader" //set journal meta
	delWalHeaderType            //= "delWalHeader" //set journal meta
	manifestSnapshotType        //= "filesSnapshot"
	FilesIndexType

	segmentExt            = ".seg"
	manifestExt           = ".log"
	manifestJournalExt    = ".mlog"
	manifestJournalExtTmp = ".mlog.tmp"
)

func openManifest(manifestDir string, segmentDir string, journalDir string) (*manifest, error) {
	files := &manifest{
		l:              sync.RWMutex{},
		maxJournalSize: 128 * MB,
		journal:        nil,
		segmentDir:     segmentDir,
		manifestDir:    manifestDir,
		journalDir:     journalDir,
		inRecovery:     false,
		ManifestSnapshot: &pb.ManifestSnapshot{
			FileIndex:      &pb.FileIndex{},
			Version:        &pb.Version{},
			Segments:       nil,
			Journals:       nil,
			JournalHeaders: map[string]*pb.JournalMeta{},
		},
		notifyCompactionLog: make(chan interface{}, 1),
		c:                   make(chan interface{}, 1),
		s:                   make(chan interface{}, 1),
	}
	if err := files.reload(); err != nil {
		return nil, err
	}
	return files, nil
}

func copyStrings(strings []string) []string {
	return append(make([]string, 0, len(strings)), strings...)
}

func (m *manifest) getSegmentFiles() []string {
	m.l.RLock()
	defer m.l.RUnlock()
	return copyStrings(m.Segments)
}

func (m *manifest) getJournalFiles() []string {
	m.l.RLock()
	defer m.l.RUnlock()
	return copyStrings(m.Journals)
}

func (m *manifest) reload() error {
	m.inRecovery = true
	defer func() {
		m.inRecovery = false
	}()
	var logFiles []string
	err := filepath.Walk(m.manifestDir, func(path string, info os.FileInfo, err error) error {
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
		m.journal, err = openJournal(filepath.Join(m.manifestDir, "1"+manifestJournalExt))
	} else {
		m.journal, err = openJournal(logFiles[len(logFiles)-1])
		if err != nil {
			return err
		}
	}
	err = m.journal.Read(func(e *WriteRequest) error {
		m.Version = e.Entry.Ver
		switch e.Entry.StreamID {
		case appendSegmentType:
			var appendS pb.AppendSegment
			if err := proto.Unmarshal(e.Entry.Data, &appendS); err != nil {
				return errors.WithStack(err)
			}
			return m.appendSegment(&appendS)
		case deleteSegmentType:
			var deleteS pb.DeleteSegment
			if err := proto.Unmarshal(e.Entry.Data, &deleteS); err != nil {
				return errors.WithStack(err)
			}
			return m.deleteSegment(&deleteS)
		case appendJournalType:
			var message pb.AppendJournal
			if err := proto.Unmarshal(e.Entry.Data, &message); err != nil {
				return errors.WithStack(err)
			}
			return m.AppendJournal(&message)
		case deleteJournalType:
			var message pb.DeleteJournal
			if err := proto.Unmarshal(e.Entry.Data, &message); err != nil {
				return errors.WithStack(err)
			}
			return m.deleteJournal(&message)
		case manifestSnapshotType:
			if err := proto.Unmarshal(e.Entry.Data, m); err != nil {
				return errors.WithStack(err)
			}
		case setJournalHeaderType:
			var header pb.JournalMeta
			if err := proto.Unmarshal(e.Entry.Data, &header); err != nil {
				return errors.WithStack(err)
			}
			return m.setJournalHeader(&header)
		case FilesIndexType:
			var fileIndex pb.FileIndex
			if err := proto.Unmarshal(e.Entry.Data, &fileIndex); err != nil {
				return errors.WithStack(err)
			}
			m.FileIndex = &fileIndex
		case delWalHeaderType:
			var message pb.DelJournalHeader
			if err := proto.Unmarshal(e.Entry.Data, &message); err != nil {
				return err
			}
			return m.delWalHeader(&message)
		default:
			log.Fatalf("unknown type %d", e.Entry.StreamID)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *manifest) getSegmentIndex() int64 {
	m.l.Lock()
	defer m.l.Unlock()
	return m.FileIndex.SegmentIndex
}

func (m *manifest) setSegmentIndex(segmentIndex int64) error {
	m.l.Lock()
	defer m.l.Unlock()
	if m.FileIndex.SegmentIndex < segmentIndex {
		m.FileIndex.SegmentIndex = segmentIndex
	} else {
		return errors.New("error segment index")
	}
	return m.writeFilesIndex()
}

func (m *manifest) getNextSegment() (string, error) {
	m.l.Lock()
	defer m.l.Unlock()
	m.FileIndex.SegmentIndex++
	if err := m.writeFilesIndex(); err != nil {
		return "", err
	}
	return filepath.Join(m.segmentDir, strconv.FormatInt(m.FileIndex.SegmentIndex, 10)+segmentExt), nil
}

func (m *manifest) compactionLog() {
	m.l.Lock()
	defer m.l.Unlock()
	if m.journal.Size() < m.maxJournalSize {
		return
	}
	m.FileIndex.SegmentIndex++
	m.Version.Index++
	tmpJournal := strconv.FormatInt(m.FileIndex.SegmentIndex, 10) + manifestJournalExtTmp
	tmpJournal = filepath.Join(m.manifestDir, tmpJournal)
	journal, err := openJournal(tmpJournal)
	if err != nil {
		log.Fatal(err)
	}
	data, err := proto.Marshal(m.ManifestSnapshot)
	if err != nil {
		log.Fatal(err)
	}
	if err := journal.Write(&WriteRequest{
		Entry: &pb.Entry{
			StreamID: manifestSnapshotType,
			Offset:   0,
			Data:     data,
			Ver:      m.Version,
		},
		close: false,
		end:   0,
		err:   nil,
		cb:    nil,
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
	if err := m.journal.Flush(); err != nil {
		log.Fatal(err)
	}
	if err := m.journal.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.Remove(m.journal.Filename()); err != nil {
		log.Fatal(err)
	}
	m.journal, err = openJournal(filename)
	if err != nil {
		log.Fatal(err)
	}
}

func (m *manifest) AppendJournal(appendJournal *pb.AppendJournal) error {
	m.l.Lock()
	defer m.l.Unlock()
	filename := filepath.Base(appendJournal.Filename)
	for _, file := range m.Journals {
		if file == filename {
			return errors.Errorf("journal filename repeated")
		}
	}
	m.Journals = append(m.Journals, filename)
	sortIntFilename(m.Journals)
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(appendJournal)
	return m.writeEntry(appendJournalType, data)
}

func (m *manifest) deleteJournal(deleteJournal *pb.DeleteJournal) error {
	m.l.Lock()
	defer m.l.Unlock()
	filename := filepath.Base(deleteJournal.Filename)
	var find = false
	for index, file := range m.Journals {
		if file == filename {
			copy(m.Journals[index:], m.Journals[index+1:])
			m.Journals = m.Journals[:len(m.Journals)-1]
			find = true
			break
		}
	}
	if find == false {
		return errors.Errorf("no find journal file [%s]", filename)
	}
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(deleteJournal)
	return m.writeEntry(deleteJournalType, data)
}

func (m *manifest) appendSegment(appendS *pb.AppendSegment) error {
	m.l.Lock()
	defer m.l.Unlock()
	filename := filepath.Base(appendS.Filename)
	for _, file := range m.Segments {
		if file == filename {
			return errors.Errorf("segment filename repeated")
		}
	}
	m.Segments = append(m.Segments, filename)
	sortIntFilename(m.Segments)
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(appendS)
	return m.writeEntry(appendSegmentType, data)
}

func (m *manifest) setJournalHeader(header *pb.JournalMeta) error {
	m.l.Lock()
	defer m.l.Unlock()
	m.JournalHeaders[filepath.Base(header.Filename)] = header
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(header)
	return m.writeEntry(setJournalHeaderType, data)
}

func (m *manifest) getJournalHeader(filename string) (*pb.JournalMeta, error) {
	m.l.Lock()
	defer m.l.Unlock()
	header, ok := m.JournalHeaders[filename]
	if !ok {
		return header, errors.Errorf("no find meta [%s]", filename)
	}
	return header, nil
}

func (m *manifest) delWalHeader(header *pb.DelJournalHeader) error {
	m.l.Lock()
	defer m.l.Unlock()
	_, ok := m.JournalHeaders[filepath.Base(header.Filename)]
	if ok == false {
		return errors.Errorf("no find journal [%s]", header.Filename)
	}
	delete(m.JournalHeaders, filepath.Base(header.Filename))
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(header)
	return m.writeEntry(delWalHeaderType, data)
}

func (m *manifest) deleteSegment(deleteS *pb.DeleteSegment) error {
	m.l.Lock()
	defer m.l.Unlock()
	filename := filepath.Base(deleteS.Filename)
	var find = false
	for index, file := range m.Segments {
		if file == filename {
			copy(m.Segments[index:], m.Segments[index+1:])
			m.Segments = m.Segments[:len(m.Segments)-1]
			find = true
			break
		}
	}
	if find == false {
		return errors.Errorf("no find segment [%s]", filename)
	}
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(deleteS)
	return m.writeEntry(deleteSegmentType, data)
}

func (m *manifest) writeEntry(typ int64, data []byte, ) error {
	m.Version.Index++
	if err := m.journal.Write(&WriteRequest{
		Entry: &pb.Entry{
			StreamID: typ,
			Offset:   0,
			Data:     data,
			Ver:      m.Version,
		},
		close: false,
		end:   0,
		err:   nil,
		cb:    nil,
	}); err != nil {
		return err
	}
	if err := m.journal.Flush(); err != nil {
		return err
	}
	if m.journal.Size() > m.maxJournalSize {
		m.tryCompactionLog()
	}
	return nil
}

func (m *manifest) tryCompactionLog() {
	select {
	case m.notifyCompactionLog <- struct{}{}:
	default:
	}
}

func (m *manifest) close() {
	m.l.Lock()
	defer m.l.Unlock()
	_ = m.journal.Close()
	close(m.c)
	<-m.s
}

func (m *manifest) start() {
	go func() {
		for {
			select {
			case <-m.c:
				close(m.s)
				return
			case <-m.notifyCompactionLog:
				m.compactionLog()
			}
		}
	}()
}

func (m *manifest) writeFilesIndex() error {
	data, err := proto.Marshal(m.FileIndex)
	if err != nil {
		panic(err)
	}
	return m.writeEntry(FilesIndexType, data)
}

func (m *manifest) geNextJournal() (string, error) {
	m.l.Lock()
	defer m.l.Unlock()
	m.FileIndex.JournalIndex++
	if err := m.writeFilesIndex(); err != nil {
		return "", err
	}
	return filepath.Join(m.journalDir, strconv.FormatInt(m.FileIndex.JournalIndex, 10)+manifestExt), nil
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
