// Copyright 2015 The etcd Authors
// Copyright 2020-2026 The streamIO Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raftnode

import (
	"encoding/json"
	"io"
	"log"
	"sort"

	"go.etcd.io/etcd/etcdserver/api/membership"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/logutil"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

func StartNode(cfg Config, cl *membership.RaftCluster, ids []types.ID) (id types.ID, n raft.Node, s *raft.MemoryStorage, w *wal.WAL) {
	var err error
	member := cl.MemberByName(cfg.Name)
	metadata := pbutil.MustMarshal(
		&pb.Metadata{
			NodeID:    uint64(member.ID),
			ClusterID: uint64(cl.ID()),
		},
	)
	if w, err = wal.Create(cfg.Logger, cfg.WALDir(), metadata); err != nil {
		cfg.Logger.Panic("failed to create WAL", zap.Error(err))
	}
	peers := make([]raft.Peer, len(ids))
	for i, id := range ids {
		var ctx []byte
		ctx, err = json.Marshal((*cl).Member(id))
		if err != nil {
			cfg.Logger.Panic("failed to marshal member", zap.Error(err))
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}
	id = member.ID
	cfg.Logger.Info(
		"starting local member",
		zap.String("local-member-id", id.String()),
		zap.String("cluster-id", cl.ID().String()),
	)
	s = raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	if cfg.Logger != nil {
		// called after capnslog setting in "init" function
		if cfg.LoggerConfig != nil {
			c.Logger, err = logutil.NewRaftLogger(cfg.LoggerConfig)
			if err != nil {
				log.Fatalf("cannot create raft logger %v", err)
			}
		} else if cfg.LoggerCore != nil && cfg.LoggerWriteSyncer != nil {
			c.Logger = logutil.NewRaftLoggerFromZapCore(cfg.LoggerCore, cfg.LoggerWriteSyncer)
		}
	}

	if len(peers) == 0 {
		n = raft.RestartNode(c)
	} else {
		n = raft.StartNode(c, peers)
	}
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return id, n, s, w
}

func restartAsStandaloneNode(cfg Config, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, id, cid, st, ents := readWAL(cfg.Logger, cfg.WALDir(), walsnap)

	// discard the previously uncommitted entries
	for i, ent := range ents {
		if ent.Index > st.Commit {
			cfg.Logger.Info(
				"discarding uncommitted WAL entries",
				zap.Uint64("entry-index", ent.Index),
				zap.Uint64("commit-index-from-wal", st.Commit),
				zap.Int("number-of-discarded-entries", len(ents)-i),
			)
			ents = ents[:i]
			break
		}
	}

	// force append the configuration change entries
	toAppEnts := createConfigChangeEnts(
		cfg.Logger,
		getIDs(cfg.Logger, snapshot, ents),
		uint64(id),
		st.Term,
		st.Commit,
	)
	ents = append(ents, toAppEnts...)

	// force commit newly appended entries
	err := w.Save(raftpb.HardState{}, toAppEnts)
	if err != nil {
		cfg.Logger.Fatal("failed to save hard state and entries", zap.Error(err))
	}
	if len(ents) != 0 {
		st.Commit = ents[len(ents)-1].Index
	}

	cfg.Logger.Info(
		"forcing restart member",
		zap.String("cluster-id", cid.String()),
		zap.String("local-member-id", id.String()),
		zap.Uint64("commit-index", st.Commit),
	)

	cl := membership.NewCluster(cfg.Logger, "")
	cl.SetID(id, cid)
	s := raft.NewMemoryStorage()
	if snapshot != nil {
		s.ApplySnapshot(*snapshot)
	}
	s.SetHardState(st)
	s.Append(ents)
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	if cfg.Logger != nil {
		// called after capnslog setting in "init" function
		if cfg.LoggerConfig != nil {
			c.Logger, err = logutil.NewRaftLogger(cfg.LoggerConfig)
			if err != nil {
				log.Fatalf("cannot create raft logger %v", err)
			}
		} else if cfg.LoggerCore != nil && cfg.LoggerWriteSyncer != nil {
			c.Logger = logutil.NewRaftLoggerFromZapCore(cfg.LoggerCore, cfg.LoggerWriteSyncer)
		}
	}

	n := raft.RestartNode(c)
	raftStatus = n.Status
	return id, cl, n, s, w
}

// getIDs returns an ordered set of IDs included in the given snapshot and
// the entries. The given snapshot/entries can contain two kinds of
// ID-related entry:
// - ConfChangeAddNode, in which case the contained ID will be added into the set.
// - ConfChangeRemoveNode, in which case the contained ID will be removed from the set.
func getIDs(lg *zap.Logger, snap *raftpb.Snapshot, ents []raftpb.Entry) []uint64 {
	ids := make(map[uint64]bool)
	if snap != nil {
		for _, id := range snap.Metadata.ConfState.Voters {
			ids[id] = true
		}
	}
	for _, e := range ents {
		if e.Type != raftpb.EntryConfChange {
			continue
		}
		var cc raftpb.ConfChange
		pbutil.MustUnmarshal(&cc, e.Data)
		switch cc.Type {
		case raftpb.ConfChangeAddNode:
			ids[cc.NodeID] = true
		case raftpb.ConfChangeRemoveNode:
			delete(ids, cc.NodeID)
		case raftpb.ConfChangeUpdateNode:
			// do nothing
		default:
			lg.Panic("unknown ConfChange Type", zap.String("type", cc.Type.String()))
		}
	}
	sids := make(types.Uint64Slice, 0, len(ids))
	for id := range ids {
		sids = append(sids, id)
	}
	sort.Sort(sids)
	return []uint64(sids)
}

// createConfigChangeEnts creates a series of Raft entries (i.e.
// EntryConfChange) to remove the set of given IDs from the cluster. The ID
// `self` is _not_ removed, even if present in the set.
// If `self` is not inside the given ids, it creates a Raft entry to add a
// default member with the given `self`.
func createConfigChangeEnts(lg *zap.Logger, ids []uint64, self uint64, term, index uint64) []raftpb.Entry {
	found := false
	for _, id := range ids {
		if id == self {
			found = true
		}
	}

	var ents []raftpb.Entry
	next := index + 1

	// NB: always add self first, then remove other nodes. Raft will panic if the
	// set of voters ever becomes empty.
	if !found {
		m := membership.Member{
			ID:             types.ID(self),
			RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"http://localhost:2380"}},
		}
		ctx, err := json.Marshal(m)
		if err != nil {
			lg.Panic("failed to marshal member", zap.Error(err))
		}
		cc := &raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  self,
			Context: ctx,
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  pbutil.MustMarshal(cc),
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
		next++
	}

	for _, id := range ids {
		if id == self {
			continue
		}
		cc := &raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: id,
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  pbutil.MustMarshal(cc),
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
		next++
	}

	return ents
}

func RestartNode(cfg Config, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, id, cid, st, ents := readWAL(cfg.Logger, cfg.WALDir(), walsnap)

	cfg.Logger.Info(
		"restarting local member",
		zap.String("cluster-id", cid.String()),
		zap.String("local-member-id", id.String()),
		zap.Uint64("commit-index", st.Commit),
	)
	cl := membership.NewCluster(cfg.Logger, "")
	cl.SetID(id, cid)
	s := raft.NewMemoryStorage()
	if snapshot != nil {
		s.ApplySnapshot(*snapshot)
	}
	s.SetHardState(st)
	s.Append(ents)
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	if cfg.Logger != nil {
		// called after capnslog setting in "init" function
		var err error
		if cfg.LoggerConfig != nil {
			c.Logger, err = logutil.NewRaftLogger(cfg.LoggerConfig)
			if err != nil {
				log.Fatalf("cannot create raft logger %v", err)
			}
		} else if cfg.LoggerCore != nil && cfg.LoggerWriteSyncer != nil {
			c.Logger = logutil.NewRaftLoggerFromZapCore(cfg.LoggerCore, cfg.LoggerWriteSyncer)
		}
	}

	n := raft.RestartNode(c)
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return id, cl, n, s, w
}

// readWAL reads the WAL at the given snap and returns the wal, its latest HardState and cluster ID, and all entries that appear
// after the position of the given snap in the WAL.
// The snap must have been previously saved to the WAL, or this call will panic.
func readWAL(lg *zap.Logger, waldir string, snap walpb.Snapshot) (w *wal.WAL, id, cid types.ID, st raftpb.HardState, ents []raftpb.Entry) {
	var (
		err       error
		wmetadata []byte
	)

	repaired := false
	for {
		if w, err = wal.Open(lg, waldir, snap); err != nil {
			lg.Fatal("failed to open WAL", zap.Error(err))
		}
		if wmetadata, st, ents, err = w.ReadAll(); err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				lg.Fatal("failed to read WAL, cannot be repaired", zap.Error(err))
			}
			if !wal.Repair(lg, waldir) {
				lg.Fatal("failed to repair WAL", zap.Error(err))
			} else {
				lg.Info("repaired WAL", zap.Error(err))
				repaired = true
			}
			continue
		}
		break
	}
	var metadata pb.Metadata
	pbutil.MustUnmarshal(&metadata, wmetadata)
	id = types.ID(metadata.NodeID)
	cid = types.ID(metadata.ClusterID)
	return w, id, cid, st, ents
}
