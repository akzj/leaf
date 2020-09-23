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
	locker              sync.RWMutex
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
	FilesIDType

	segmentExt            = ".seg"
	manifestExt           = ".log"
	manifestJournalExt    = ".manifest"
	manifestJournalExtTmp = ".manifest.tmp"
)

func OpenManifest(manifestDir string, segmentDir string, journalDir string) (*Manifest, error) {
	manifest := &Manifest{
		locker:         sync.RWMutex{},
		maxJournalSize: 128 * MB,
		journal:        nil,
		segmentDir:     segmentDir,
		manifestDir:    manifestDir,
		journalDir:     journalDir,
		inRecovery:     false,
		ManifestSnapshot: &pb.ManifestSnapshot{
			FileID:       &pb.FileID{},
			Version:      &pb.Version{},
			Segments:     nil,
			Journals:     nil,
			JournalMetas: map[string]*pb.JournalMeta{},
		},
		notifyCompactionLog: make(chan interface{}, 1),
		c:                   make(chan interface{}, 1),
		s:                   make(chan interface{}, 1),
	}
	if err := manifest.init(); err != nil {
		return nil, err
	}
	go manifest.start()
	return manifest, nil
}

func (m *Manifest) init() error {
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
			var message pb.AppendSegment
			if err := proto.Unmarshal(entry.Data, &message); err != nil {
				return errors.WithStack(err)
			}
			return m.AppendSegment(&message)
		case deleteSegmentType:
			var message pb.DeleteSegment
			if err := proto.Unmarshal(entry.Data, &message); err != nil {
				return errors.WithStack(err)
			}
			return m.DeleteSegment(&message)
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
			var message pb.JournalMeta
			if err := proto.Unmarshal(entry.Data, &message); err != nil {
				return errors.WithStack(err)
			}
			return m.SetJournalMeta(&message)
		case FilesIDType:
			var message pb.FileID
			if err := proto.Unmarshal(entry.Data, &message); err != nil {
				return errors.WithStack(err)
			}
			m.FileID = &message
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
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.journal.Size() < m.maxJournalSize {
		return nil
	}
	m.FileID.SegmentID++
	m.Version.Index++
	tmpJournal := strconv.FormatInt(m.FileID.SegmentID, 10) + manifestJournalExtTmp
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
	m.locker.RLock()
	defer m.locker.RUnlock()
	segments := CopyStrings(m.Segments)
	sortFilename(segments)
	return segments
}

func (m *Manifest) GetJournalFiles() []string {
	m.locker.RLock()
	defer m.locker.RUnlock()
	journals := CopyStrings(m.Journals)
	sortFilename(journals)
	return journals
}

func (m *Manifest) GetSegmentID() int64 {
	m.locker.Lock()
	defer m.locker.Unlock()
	return m.FileID.SegmentID
}

func (m *Manifest) SetSegmentID(ID int64) error {
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.FileID.SegmentID < ID {
		m.FileID.SegmentID = ID
	} else {
		return errors.New("error segment ID")
	}
	return m.writeFilesID()
}

func (m *Manifest) GetNextSegment() (string, error) {
	m.locker.Lock()
	defer m.locker.Unlock()
	m.FileID.SegmentID++
	if err := m.writeFilesID(); err != nil {
		return "", err
	}
	return filepath.Join(m.segmentDir, strconv.FormatInt(m.FileID.SegmentID, 10)+segmentExt), nil
}

func (m *Manifest) AppendJournal(appendJournal *pb.AppendJournal) error {
	m.locker.Lock()
	defer m.locker.Unlock()
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
	m.locker.Lock()
	defer m.locker.Unlock()
	filename := filepath.Base(deleteJournal.Filename)
	var find = false
	for ID, file := range m.Journals {
		if file == filename {
			copy(m.Journals[ID:], m.Journals[ID+1:])
			m.Journals = m.Journals[:len(m.Journals)-1]
			find = true
			break
		}
	}
	if find == false {
		return errors.Errorf("no Find journal file [%s]", filename)
	}
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(deleteJournal)
	return m.writeEntry(deleteJournalType, data)
}

func (m *Manifest) AppendSegment(appendS *pb.AppendSegment) error {
	m.locker.Lock()
	defer m.locker.Unlock()
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
	m.locker.Lock()
	defer m.locker.Unlock()
	m.JournalMetas[filepath.Base(header.Filename)] = header
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(header)
	return m.writeEntry(setJournalMetaType, data)
}

func (m *Manifest) GetJournalMetas() []*pb.JournalMeta {
	m.locker.Lock()
	defer m.locker.Unlock()
	var journalMetas []*pb.JournalMeta
	for _, journalMeta := range m.JournalMetas {
		journalMetas = append(journalMetas, journalMeta)
	}
	return journalMetas
}

func (m *Manifest) GetJournalMeta(filename string) (*pb.JournalMeta, error) {
	m.locker.Lock()
	defer m.locker.Unlock()
	header, ok := m.JournalMetas[filename]
	if !ok {
		return header, errors.Errorf("no Find meta [%s]", filename)
	}
	return header, nil
}

func (m *Manifest) DelJournalMeta(header *pb.DelJournalMeta) error {
	m.locker.Lock()
	defer m.locker.Unlock()
	_, ok := m.JournalMetas[filepath.Base(header.Filename)]
	if ok == false {
		return errors.Errorf("no Find journal [%s]", header.Filename)
	}
	delete(m.JournalMetas, filepath.Base(header.Filename))
	if m.inRecovery {
		return nil
	}
	data, _ := proto.Marshal(header)
	return m.writeEntry(delJournalMetaType, data)
}

func (m *Manifest) DeleteSegment(deleteS *pb.DeleteSegment) error {
	m.locker.Lock()
	defer m.locker.Unlock()
	filename := filepath.Base(deleteS.Filename)
	var find = false
	for i, file := range m.Segments {
		if file == filename {
			copy(m.Segments[i:], m.Segments[i+1:])
			m.Segments = m.Segments[:len(m.Segments)-1]
			find = true
			break
		}
	}
	if find == false {
		return errors.Errorf("no Find segment [%s]", filename)
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
	m.locker.Lock()
	defer m.locker.Unlock()
	if err := m.journal.Close(); err != nil {
		return err
	}
	close(m.c)
	<-m.s
	return nil
}

func (m *Manifest) writeFilesID() error {
	data, err := proto.Marshal(m.FileID)
	if err != nil {
		panic(err)
	}
	return m.writeEntry(FilesIDType, data)
}

func (m *Manifest) NextJournal() (string, error) {
	m.locker.Lock()
	defer m.locker.Unlock()
	m.FileID.JournalID++
	if err := m.writeFilesID(); err != nil {
		return "", err
	}
	return filepath.Join(m.journalDir, strconv.FormatInt(m.FileID.JournalID, 10)+manifestExt), nil
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

func parseFileID(filename string) (int64, error) {
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
