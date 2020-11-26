package raftrpc

import (
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"time"
)

type peer struct {
}

func (p *peer) send(m raftpb.Message) {
	panic("implement me")
}

func (p *peer) sendSnap(m snap.Message) {
	panic("implement me")
}

func (p *peer) update(urls types.URLs) {
	panic("implement me")
}

func (p *peer) attachOutgoingConn(conn *interface{}) {
	panic("implement me")
}

func (p *peer) activeSince() time.Time {
	panic("implement me")
}

func (p *peer) stop() {
	panic("implement me")
}
