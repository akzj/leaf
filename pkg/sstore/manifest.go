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

type Manifest struct {
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
	appendSegmentType = iota
	deleteSegmentType
	appendJournalType
	deleteJournalType
	setJournalMetaType
	delJournalMetaType
	manifestSnapshotType
	FilesIndexType

	segmentExt            = ".seg"
	manifestExt           = ".log"
	manifestJournalExt    = ".manifest"
	manifestJournalExtTmp = ".manifest.tmp"
)

func OpenManifest(manifestDir string, segmentDir string, journalDir string) (*Manifest, error) {
	manifest := &Manifest{
		l:              sync.RWMutex{},
		maxJournalSize: 128 * MB,
		journal:        nil,
		segmentDir:     segmentDir,
		manifestDir:    manifestDir,
		journalDir:     journalDir,
		inRecovery:     false,
		ManifestSnapshot: &pb.ManifestSnapshot{
			FileIndex:    &pb.FileIndex{},
			Version:      &pb.Version{},
			Segments:     nil,
			Journals:     nil,
			JournalMetas: map[string]*pb.JournalMeta{},
		},
		notifyCompactionLog: make(chan interface{}, 1),
		c:                   make(chan interface{}, 1),
		s:                   make(chan interface{}, 1),
	}
	if err := manifest.reload(); err != nil {
		return nil, err
	}
	go manifest.start()
	return manifest, nil
}

func (m *Manifest) reload() error {
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

	sortFilename(logFiles)
	if len(logFiles) == 0 {
		m.journal, err = OpenJournal(filepath.Join(m.manifestDir, "1"+manifestJournalExt))
	} else {
		m.journal, err = OpenJournal(logFiles[len(logFiles)-1])
		if err != nil {
			return err
		}
	}
	err = m.journal.Range(func(entry *pb.Entry) error {
		m.Version = entry.Ver
		switch entry.StreamID {
		case appendSegmentType:
			var appendS pb.AppendSegment
			if err := proto.Unmarshal(entry.Data, &appendS); err != nil {
				return errors.WithStack(err)
			}
			return m.AppendSegment(&appendS)
		case deleteSegmentType:
			var deleteS pb.DeleteSegment
			if err := proto.Unmarshal(entry.Data, &deleteS); err != nil {
				return errors.WithStack(err)
			}
			return m.DeleteSegment(&deleteS)
		case appendJournalType:
			var message pb.AppendJournal
			if err := proto.Unmarshal(entry.Data, &message); err != nil {
				return errors.WithStack(err)
			}
			return m.AppendJournal(&message)
		case deleteJournalType:
			var message pb.DeleteJournal
			if err := proto.Unmarshal(entry.Data, &message); err != nil {
				return errors.WithStack(err)
			}
			return m.DeleteJournal(&message)
		case manifestSnapshotType:
			if err := proto.Unmarshal(entry.Data, m); err != nil {
				return errors.WithStack(err)
			}
		case setJournalMetaType:
			var header pb.JournalMeta
			if err := proto.Unmarshal(entry.Data, &header); err != nil {
				return errors.WithStack(err)
			}
			return m.SetJournalMeta(&header)
		case FilesIndexType:
			var fileIndex pb.FileIndex
			if err := proto.Unmarshal(entry.Data, &fileIndex); err != nil {
				return errors.WithStack(err)
			}
			m.FileIndex = &fileIndex
		case delJournalMetaType:
			var message pb.DelJournalMeta
			if err := proto.Unmarshal(entry.Data, &message); err != nil {
				return err
			}
			return m.DelJournalMeta(&message)
		default:
			log.Fatalf("unknown type %d", entry.StreamID)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *Manifest) compactionLog() error {
	m.l.Lock()
	defer m.l.Unlock()
	if m.journal.Size() < m.maxJournalSize {
		return nil
	}
	m.FileIndex.SegmentIndex++
	m.Version.Index++
	tmpJournal := strconv.FormatInt(m.FileIndex.SegmentIndex, 10) + manifestJournalExtTmp
	tmpJournal = filepath.Join(m.manifestDir, tmpJournal)
	journal, err := OpenJournal(tmpJournal)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(m.ManifestSnapshot)
	if err != nil {
		return err
	}
	if err := journal.Write(&pb.Entry{
		StreamID: manifestSnapshotType,
		Offset:   0,
		Data:     data,
		Ver:      m.Version,
	}); err != nil {
		return err
	}
	if err := journal.Flush(); err != nil {
		return err
	}
	if err := journal.Close(); err != nil {
		return err
	}
	if err := m.journal.Close(); err != nil {
		log.Fatal(err)
	}
	filename := strings.ReplaceAll(tmpJournal, manifestJournalExtTmp, manifestJournalExt)
	if err := os.Rename(tmpJournal, filename); err != nil {
		return errors.WithStack(err)
	}
	m.journal, err = OpenJournal(filename)
	if err != nil {
		return err
	}
	return nil
}

func CopyStrings(strings []string) []string {
	return append(make([]string, 0, len(strings)), strings...)
}

func (m *Manifest) GetSegmentFiles() []string {
	m.l.RLock()
	defer m.l.RUnlock()
	segments := CopyStrings(m.Segments)
	sortFilename(segments)
	return segments
}

func (m *Manifest) GetJournalFiles() []string {
	m.l.RLock()
	defer m.l.RUnlock()
	journals := CopyStrings(m.Journals)
	sortFilename(journals)
	return journals
}

func (m *Manifest) GetSegmentIndex() int64 {
	m.l.Lock()
	defer m.l.Unlock()
	return m.FileIndex.SegmentIndex
}

func (m *Manifest) SetSegmentIndex(segmentIndex int64) error {
	m.l.Lock()
	defer m.l.Unlock()
	if m.FileIndex.SegmentIndex < segmentIndex {
		m.FileIndex.SegmentIndex = segmentIndex
	} else {
		return errors.New("error segment index")
	}
	return m.writeFilesIndex()
}

func (m *Manifest) GetNextSegment() (string, error) {
	m.l.Lock()
	defer m.l.Unlock()
	m.FileIndex.SegmentIndex++
	if err := m.writeFilesIndex(); err != nil {
		return "", err
	}
	return filepath.Join(m.segmentDir, strconv.FormatInt(m.FileIndex.SegmentIndex, 10)+segmentExt), nil
}

func (m *Manifest) AppendJournal(appendJournal *pb.AppendJournal) error {
	m.l.Lock()
	defer m.l.Unlock()
	filename := filepath.Base(appendJournal.Filename)
	for _, file := range m.Journals {
		if file == filename {
			return errors.Errorf("journal filename repeated")
		}
	}
	m.Journals = append(m.Journals, filename)
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(appendJournal)
	return m.writeEntry(appendJournalType, data)
}

func (m *Manifest) DeleteJournal(deleteJournal *pb.DeleteJournal) error {
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

func (m *Manifest) AppendSegment(appendS *pb.AppendSegment) error {
	m.l.Lock()
	defer m.l.Unlock()
	filename := filepath.Base(appendS.Filename)
	for _, file := range m.Segments {
		if file == filename {
			return errors.Errorf("segment filename repeated")
		}
	}
	m.Segments = append(m.Segments, filename)
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(appendS)
	return m.writeEntry(appendSegmentType, data)
}

func (m *Manifest) SetJournalMeta(header *pb.JournalMeta) error {
	m.l.Lock()
	defer m.l.Unlock()
	m.JournalMetas[filepath.Base(header.Filename)] = header
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(header)
	return m.writeEntry(setJournalMetaType, data)
}

func (m *Manifest) GetJournalMetas() []*pb.JournalMeta {
	m.l.Lock()
	defer m.l.Unlock()
	var journalMetas []*pb.JournalMeta
	for _, journalMeta := range m.JournalMetas {
		journalMetas = append(journalMetas, journalMeta)
	}
	return journalMetas
}

func (m *Manifest) GetJournalMeta(filename string) (*pb.JournalMeta, error) {
	m.l.Lock()
	defer m.l.Unlock()
	header, ok := m.JournalMetas[filename]
	if !ok {
		return header, errors.Errorf("no find meta [%s]", filename)
	}
	return header, nil
}

func (m *Manifest) DelJournalMeta(header *pb.DelJournalMeta) error {
	m.l.Lock()
	defer m.l.Unlock()
	_, ok := m.JournalMetas[filepath.Base(header.Filename)]
	if ok == false {
		return errors.Errorf("no find journal [%s]", header.Filename)
	}
	delete(m.JournalMetas, filepath.Base(header.Filename))
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(header)
	return m.writeEntry(delJournalMetaType, data)
}

func (m *Manifest) DeleteSegment(deleteS *pb.DeleteSegment) error {
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

func (m *Manifest) writeEntry(typ int64, data []byte, ) error {
	m.Version.Index++
	if err := m.journal.Write(&pb.Entry{
		StreamID: typ,
		Offset:   0,
		Data:     data,
		Ver:      m.Version,
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

func (m *Manifest) tryCompactionLog() {
	select {
	case m.notifyCompactionLog <- struct{}{}:
	default:
	}
}

func (m *Manifest) Close() error {
	m.l.Lock()
	defer m.l.Unlock()
	if err := m.journal.Close(); err != nil {
		return err
	}
	close(m.c)
	<-m.s
	return nil
}

func (m *Manifest) writeFilesIndex() error {
	data, err := proto.Marshal(m.FileIndex)
	if err != nil {
		panic(err)
	}
	return m.writeEntry(FilesIndexType, data)
}

func (m *Manifest) NextJournal() (string, error) {
	m.l.Lock()
	defer m.l.Unlock()
	m.FileIndex.JournalIndex++
	if err := m.writeFilesIndex(); err != nil {
		return "", err
	}
	return filepath.Join(m.journalDir, strconv.FormatInt(m.FileIndex.JournalIndex, 10)+manifestExt), nil
}

func (m *Manifest) start() {
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

func parseFileIndex(filename string) (int64, error) {
	filename = filepath.Base(filename)
	token := strings.SplitN(filename, ".", 2)[0]
	return strconv.ParseInt(token, 10, 64)
}

func sortFilename(files []string) {
	sort.Slice(files, func(i, j int) bool {
		iLen := len(files[i])
		jLen := len(files[j])
		if iLen != jLen {
			return iLen < jLen
		}
		return files[i] < files[j]
	})
}
