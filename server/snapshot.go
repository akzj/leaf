package server

import (
	"github.com/akzj/sstore"
	"github.com/pkg/errors"
	"io"
	"sync"
)

type snapshot struct {
	sstore        *sstore.SStore
	streamRequest streamReader
	pipelineC     chan interface{}
}

type streamReader interface {
	Request(readStreamRequest) (io.ReadCloser, error)
}

type readStreamRequest struct {
	Begin int64  `json:"begin" uri:"begin"`
	End   int64  `json:"end" uri:"end"`
	size  int64  `json:"-" `
	Name  string `json:"name" uri:"name"`
}

func (snapshot *snapshot) makeReadStreamRequests(ss sstore.Snapshot) []readStreamRequest {
	var items []readStreamRequest
	for name, end := range ss.EndMap {
		begin, _ := snapshot.sstore.End(name)
		if begin < end {
			items = append(items, readStreamRequest{
				Begin: begin,
				End:   end,
				size:  end - begin,
				Name:  name,
			})
		}
	}
	return items
}

func (snapshot *snapshot) install(ss sstore.Snapshot) {
	syncStreamItems := snapshot.makeReadStreamRequests(ss)
	if len(syncStreamItems) == 0 {
		return
	}

	var wg sync.WaitGroup
	var errs = make(map[string]error)
	var locker sync.Mutex
	for _, item := range syncStreamItems {
		snapshot.pipelineC <- struct{}{}
		go func() {
			defer func() {
				wg.Done()
				<-snapshot.pipelineC
			}()
			if err := snapshot.doRequest(item); err != nil {
				locker.Lock()
				defer locker.Unlock()
				errs[item.Name] = err
			}
		}()
	}
	wg.Wait()
}

func (snapshot *snapshot) doRequest(request readStreamRequest) error {
	reader, err := snapshot.streamRequest.Request(request)
	if err != nil {
		return err
	}
	var bytes int64
	var buf = make([]byte, 1024*1024)
	for {
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		bytes += int64(n)
		if n == 0 {
			break
		}
		if _, err := snapshot.sstore.Append(request.Name, buf[:n]); err != nil {
			return err
		}
	}
	if bytes != request.size {
		return errors.Errorf("read bytes[%d] != [%d]", bytes, request.size)
	}
	return nil
}
