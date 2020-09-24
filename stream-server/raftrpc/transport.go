package raftrpc

import (
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"net/http"
	"time"
)

type transport struct {
	rafthttp.Transporter
}

func (t *transport) Start() error {
	panic("implement me")
}

func (t *transport) Handler() http.Handler {
	panic("implement me")
}

func (t *transport) Send(m []raftpb.Message) {
	panic("implement me")
}

func (t *transport) SendSnapshot(m snap.Message) {
	panic("implement me")
}

func (t *transport) AddRemote(id types.ID, urls []string) {
	panic("implement me")
}

func (t *transport) AddPeer(id types.ID, urls []string) {
	panic("implement me")
}

func (t *transport) RemovePeer(id types.ID) {
	panic("implement me")
}

func (t *transport) RemoveAllPeers() {
	panic("implement me")
}

func (t *transport) UpdatePeer(id types.ID, urls []string) {
	panic("implement me")
}

func (t *transport) ActiveSince(id types.ID) time.Time {
	panic("implement me")
}

func (t *transport) ActivePeers() int {
	panic("implement me")
}

func (t *transport) Stop() {
	panic("implement me")
}
