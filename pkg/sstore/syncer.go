package sstore

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	block_queue "github.com/akzj/streamIO/pkg/block-queue"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type Syncer struct {
	sstore *SStore
	queue  *block_queue.Queue

	journalLocker sync.Mutex
	journals      []*journal

	subscribersLocker sync.Mutex
	subscribers       map[int64]*subscriber
}

type subscriber struct {
	nextIndex *int64
	bytes     int64
	queue     *block_queue.QueueWithContext
}

type jIndex struct {
	Offset int64
	Index  int64
}

func (JI *jIndex) String() string {
	return fmt.Sprintf("index:%d,offset:%d", JI.Index, JI.Offset)
}

type journalIndex struct {
	locker   sync.RWMutex
	jIndexes []jIndex
}

type SyncCallback struct {
	Index      *int64
	Segment    *SegmentReader
	Entry      *pb.Entry
	EntryQueue *block_queue.QueueWithContext
}

func newSubscriber(ctx context.Context, index *int64) *subscriber {
	return &subscriber{
		nextIndex: index,
		bytes:     0,
		queue:     block_queue.NewQueueWithContext(ctx, 1),
	}
}

func newSyncer(sstore *SStore) *Syncer {
	return &Syncer{
		sstore:            sstore,
		queue:             block_queue.NewQueue(10240),
		journalLocker:     sync.Mutex{},
		journals:          nil,
		subscribersLocker: sync.Mutex{},
		subscribers:       map[int64]*subscriber{},
	}
}
func (syncer *Syncer) appendJournal(journal *journal) {
	syncer.journalLocker.Lock()
	defer syncer.journalLocker.Unlock()
	syncer.journals = append(syncer.journals, journal)
}

func (syncer *Syncer) deleteJournal(filename string) {
	syncer.journalLocker.Lock()
	defer syncer.journalLocker.Unlock()
	for index, journal := range syncer.journals {
		if journal.filename == filename {
			journal.ref.f = func() {
				if err := os.Remove(journal.filename); err != nil {
					fmt.Println(err)
				}
			}
			journal.refDec()
			syncer.journals = append(syncer.journals[:index], syncer.journals[index+1:]...)
			break
		}
	}
}

func (syncer *Syncer) deleteSubscriber(serviceID int64) {
	syncer.subscribersLocker.Lock()
	defer syncer.subscribersLocker.Unlock()
	delete(syncer.subscribers, serviceID)
}

func (syncer *Syncer) syncJournal(ctx context.Context, index *int64,
	journal *journal, f func(callback SyncCallback) error) error {

	defer journal.refDec()
	begin, err := journal.index.find(atomic.LoadInt64(index))
	if err != nil {
		panic(err)
	}

	var reader io.Reader
	journalMMap := journal.GetJournalMMap()
	if journalMMap != nil {
		defer journalMMap.refDec()
		bufReader := bytes.NewReader(journalMMap.data[:mmapSize])
		if _, err := bufReader.Seek(begin.Offset, io.SeekStart); err != nil {
			return err
		}
		if begin.Offset >= mmapSize {
			log.Panic(begin.Offset)
		}
		reader = bufReader
	} else {
		file, err := os.Open(journal.filename)
		if err != nil {
			return err
		}
		defer func() {
			_ = file.Close()
		}()
		if _, err := file.Seek(begin.Offset, io.SeekStart); err != nil {
			return err
		}
		reader = bufio.NewReaderSize(file, 1024*1024)
	}

	var queue = block_queue.NewQueueWithContext(ctx, 1024)
	var errs = make(chan error)
	go func() {
		errs <- f(SyncCallback{EntryQueue: queue})
		fmt.Println("syncJournal done ", journal.filename)
	}()
	var count int64
	defer func() {
		fmt.Println("index", atomic.LoadInt64(index))
	}()
	for count = journal.GetFlushIndex() - atomic.LoadInt64(index); count >= 0; count-- {
		entry, err := decodeEntry(reader)
		if err != nil {
			log.Error(err)
			if err == io.EOF {
				queue.Close(nil)
				break
			}
			if err == io.ErrUnexpectedEOF {
				queue.Close(nil)
				break
			}
			return err
		}
		if entry.Ver.Index != atomic.LoadInt64(index) {
			log.Panic(*index, entry.Ver)
		}
		atomic.AddInt64(index, 1)
		if err := queue.Push(entry); err != nil {
			return err
		}
	}
	queue.Close(nil)
	select {
	case err := <-errs:
		return err
	}
}

func (syncer *Syncer) SyncRequest(ctx context.Context, serverID, index int64, f func(SyncCallback) error) error {
	fmt.Println("SyncRequest", serverID, index)
	//ssyncer from segment
	segment := syncer.sstore.committer.getSegmentByIndex(index, true)
	if segment != nil {
		defer func() {
			segment.GetSyncLocker().Unlock()
		}()
		reader, err := syncer.OpenSegmentReader(segment)
		if err != nil {
			return err
		}
		return f(SyncCallback{
			Segment: reader,
		})
	}

	defer func() {
		syncer.deleteSubscriber(serverID)
	}()

	//ssyncer from journal

	for {
		var journal *journal
		syncer.journalLocker.Lock()
		for _, journalIter := range syncer.journals {
			if index >= journalIter.meta.From.Index && index <= journalIter.GetFlushIndex() {
				fmt.Println("find", journalIter.meta, "flushIndex", journalIter.GetFlushIndex(), "index", index)
				journal = journalIter
				journal.refInc()
				break
			}
		}
		syncer.journalLocker.Unlock()
		if journal != nil {
			if err := syncer.syncJournal(ctx, &index, journal, f); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				log.Warn(ctx.Err())
				return nil
			default:
			}
		} else {
			// ssyncer from subscriber
			fmt.Println("add to subscriber")
			syncer.subscribersLocker.Lock()
			sub, ok := syncer.subscribers[serverID]
			if ok == false {
				sub = newSubscriber(ctx, &index)
				syncer.subscribers[serverID] = sub
			}
			syncer.subscribersLocker.Unlock()

		Loop:
			for {
				item, err := sub.queue.Pop()
				if err != nil {
					syncer.deleteSubscriber(serverID)
					if sub.queue.IsClose() && sub.queue.CloseErr() == errQueueFull {
						break Loop
					}
					return err
				}
				entry := item.(*WriteRequest).Entry
				if entry.Ver.Index < atomic.LoadInt64(&index) {
					continue
				} else if entry.Ver.Index == atomic.LoadInt64(&index) {
					err := f(SyncCallback{
						Index:      &index,
						Segment:    nil,
						Entry:      entry,
						EntryQueue: sub.queue,
					})
					syncer.deleteSubscriber(serverID)
					if err != nil {
						log.Warn(err)
						return err
					}
					break Loop
				} else {
					//read from journal again
					fmt.Println("//read from journal again")
					break Loop
				}
			}
		}
	}
}

func (syncer *Syncer) start() {
	go syncer.pushEntryLoop()
}

var errQueueFull = errors.New("queue full error")

func (syncer *Syncer) pushEntryLoop() {
	for {
		items := syncer.queue.PopAll(nil)
		syncer.subscribersLocker.Lock()
		if len(syncer.subscribers) == 0 {
			syncer.subscribersLocker.Unlock()
			continue
		}
		for index, sub := range syncer.subscribers {
			remain, err := sub.queue.PushManyWithBlock(items)
			if err != nil {
				delete(syncer.subscribers, index)
				continue
			}
			if len(remain) > 0 {
				sub.queue.Close(errQueueFull)
				delete(syncer.subscribers, index)
				continue
			}
		}
		syncer.subscribersLocker.Unlock()
	}
}

func (syncer *Syncer) OpenSegmentReader(segment *segment) (*SegmentReader, error) {
	f, err := os.Open(segment.filename)
	if err != nil {
		segment.refDec()
		return nil, errors.WithStack(err)
	}
	return &SegmentReader{
		f: f,
		release: func() {
			segment.refDec()
		},
	}, nil
}

func (syncer *Syncer) Close() {
	var wg sync.WaitGroup
	wg.Add(1)
	syncer.queue.Push(&closeRequest{
		cb: func() {
			wg.Done()
		},
	})
	wg.Wait()
}

func (syncer *Syncer) PushMany(requests []interface{}) {
	syncer.queue.PushMany(requests)
}

func (JI *journalIndex) append(index jIndex) {
	JI.locker.Lock()
	JI.jIndexes = append(JI.jIndexes, index)
	JI.locker.Unlock()
}

func (JI *journalIndex) find(index int64) (*jIndex, error) {
	JI.locker.RLock()
	if len(JI.jIndexes) != 0 {
		from := JI.jIndexes[0].Index
		to := JI.jIndexes[len(JI.jIndexes)-1].Index
		if index < JI.jIndexes[0].Index || index > to {
			JI.locker.RUnlock()
			return nil, errors.Errorf("index %d Out of range[%d,%d]", index, from, to)
		}
		offset := index - JI.jIndexes[0].Index
		jIndex := JI.jIndexes[offset]
		JI.locker.RUnlock()
		return &jIndex, nil
	}
	JI.locker.RUnlock()
	return nil, errors.Errorf("journal index empty")
}

type SegmentReader struct {
	f       *os.File
	release func()
}

func (s *SegmentReader) Read(p []byte) (n int, err error) {
	return s.f.Read(p)
}

func (s *SegmentReader) Seek(offset int64, whence int) (int64, error) {
	return s.f.Seek(offset, whence)
}

func (s *SegmentReader) Close() error {
	err := s.f.Close()
	s.release()
	return err
}
func (s *SegmentReader) Filename() string {
	return filepath.Base(s.f.Name())
}
func (s *SegmentReader) Size() int64 {
	stat, _ := s.f.Stat()
	return stat.Size()
}

type SegmentWriter struct {
	offset  int64
	f       *os.File
	discard func() error
	commit  func() error
}

func (s *SegmentWriter) Write(p []byte) (n int, err error) {
	n, err = s.f.Write(p)
	s.offset += int64(n)
	return
}

func (s *SegmentWriter) Discard() error {
	return s.discard()
}

func (s *SegmentWriter) Commit() error {
	return s.commit()
}

func (s *SegmentWriter) Offset() int64 {
	return s.offset
}
