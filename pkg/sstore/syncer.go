package sstore

import (
	"bufio"
	"context"
	"fmt"
	block_queue "github.com/akzj/streamIO/pkg/block-queue"
	"github.com/akzj/streamIO/pkg/sstore/pb"
	"github.com/pkg/errors"
	"io"
	"os"
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
	entries   chan *pb.Entry
}

type jIndex struct {
	Offset int64
	Index  int64
}

type journalIndex struct {
	locker   sync.RWMutex
	jIndexes []jIndex
}

type SyncCallback struct {
	Err     error
	Segment *SegmentReader
	entry   *pb.Entry
	entries chan *pb.Entry
}

func newSubscriber(index *int64) *subscriber {
	return &subscriber{
		nextIndex: index,
		bytes:     0,
		entries:   make(chan *pb.Entry, 1024*4),
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
	journal *journal, f func(callback SyncCallback)) error {

	defer journal.refDec()
	JI, err := journal.index.find(atomic.LoadInt64(index))
	if err != nil {
		panic(err)
	}
	file, err := os.Open(journal.filename)
	if err != nil {
		f(SyncCallback{Err: err})
		return err
	}
	if _, err := file.Seek(JI.Offset, io.SeekStart); err != nil {
		f(SyncCallback{Err: err})
		return err
	}
	var entries = make(chan *pb.Entry)
	go f(SyncCallback{entries: entries})
	for {
		entry, err := decodeEntry(bufio.NewReader(file))
		if err != nil {
			fmt.Println(err.Error())
			if err == io.EOF {
				return nil
			}
			if err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		select {
		case entries <- entry:
			atomic.AddInt64(index, 1)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (syncer *Syncer) SyncRequest(ctx context.Context, serverID, index int64, f func(SyncCallback)) {
	//sync from segment
	segment := syncer.sstore.committer.getSegment2(index)
	if segment != nil {
		reader, err := syncer.OpenSegmentReader(segment)
		f(SyncCallback{
			Err:     err,
			Segment: reader,
		})
		return
	}

	defer func() {
		syncer.deleteSubscriber(serverID)
	}()

	//sync from journal

	for {
		var journal *journal
		syncer.journalLocker.Lock()
		for _, journalIter := range syncer.journals {
			if journalIter.meta.From.Index <= index && index <= journalIter.meta.To.Index {
				journal = journalIter
				journal.refInc()
			}
		}
		syncer.journalLocker.Unlock()
		if journal != nil {
			if err := syncer.syncJournal(ctx, &index, journal, f); err != nil {
				return
			}
		} else {
			// sync from subscriber
			syncer.subscribersLocker.Lock()
			sub, ok := syncer.subscribers[serverID]
			if ok == false {
				sub = newSubscriber(&index)
				syncer.subscribers[serverID] = sub
			}
			syncer.subscribersLocker.Unlock()

		Loop:
			for {
				select {
				case entry := <-sub.entries:
					if entry.Ver.Index < atomic.LoadInt64(&index) {
						continue
					} else if entry.Ver.Index == atomic.LoadInt64(&index) {
						f(SyncCallback{
							Err:     nil,
							Segment: nil,
							entry:   entry,
							entries: sub.entries,
						})
						return
					} else {
						//read from journal again
						break Loop
					}
				}
			}
		}
	}
}
func (syncer *Syncer) start() {
	go syncer.pushEntryLoop()
}

func (syncer *Syncer) pushEntryLoop() {
	for {
		items := syncer.queue.PopAll(nil)
		syncer.subscribersLocker.Lock()
		if len(syncer.subscribers) == 0 {
			syncer.subscribersLocker.Unlock()
			continue
		}
		for _, item := range items {
			for index, sub := range syncer.subscribers {
				switch request := item.(type) {
				case *writeRequest:
					nextIndex := atomic.LoadInt64(sub.nextIndex)
					if nextIndex <= request.entry.Ver.Index {
						select {
						case sub.entries <- request.entry:
						default:
							close(sub.entries)
							delete(syncer.subscribers, index)
						}
					}
				case *closeRequest:
					request.cb()
					fmt.Println("syncer stop")
					return
				}
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
	err := s.Close()
	s.release()
	return err
}

type SegmentWriter struct {
	f       *os.File
	discard func() error
	commit  func() error
}

func (s *SegmentWriter) Write(p []byte) (n int, err error) {
	return s.f.Write(p)
}

func (s *SegmentWriter) Close() error {
	return s.f.Close()
}
func (s *SegmentWriter) Discard() error {
	return s.discard()
}

func (s *SegmentWriter) Commit() error {
	return s.commit()
}
