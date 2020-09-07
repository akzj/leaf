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
	"math"
	"path/filepath"
)

type Options struct {
	Path                          string `json:"path"`
	ManifestDir                   string `json:"manifest_dir"`
	WalDir                        string `json:"wal_dir"`
	SegmentDir                    string `json:"segment_dir"`
	MaxSegmentCount               int    `json:"max_segment_count"`
	BlockSize                     int    `json:"block_size"`
	MaxMStreamTableSize           int64  `json:"max_mStream_table_size"`
	MaxImmutableMStreamTableCount int    `json:"max_immutable_mStream_table_count"`
	EntryQueueCap                 int    `json:"entry_queue_cap"`
	MaxWalSize                    int64  `json:"max_wal_size"`
}

const MB = 1024 * 1024
const KB = 1024

func DefaultOptions(Path string) Options {
	return Options{
		Path:                          Path,
		ManifestDir:                   filepath.Join(Path, "manifest"),
		WalDir:                        filepath.Join(Path, "journal"),
		SegmentDir:                    filepath.Join(Path, "segment"),
		MaxSegmentCount:               math.MaxInt32,
		BlockSize:                     4 * KB,
		MaxMStreamTableSize:           256 * MB,
		MaxImmutableMStreamTableCount: 4,
		EntryQueueCap:                 128,
		MaxWalSize:                    64 * MB,
	}
}

//WithFilesDir
func (opt Options) WithFilesDir(val string) Options {
	opt.ManifestDir = val
	return opt
}

//WithSegmentDir
func (opt Options) WithSegmentDir(val string) Options {
	opt.SegmentDir = val
	return opt
}

//WithWalPath
func (opt Options) WithWalPath(val string) Options {
	opt.WalDir = val
	return opt
}

//WithMaxSegmentCount
func (opt Options) WithMaxSegmentCount(val int) Options {
	opt.MaxSegmentCount = val
	return opt
}

//WithBlockSize
func (opt Options) WithBlockSize(val int) Options {
	opt.BlockSize = val
	return opt
}

//WithMaxMStreamTableSize
func (opt Options) WithMaxMStreamTableSize(val int64) Options {
	opt.MaxMStreamTableSize = val
	return opt
}

//MaxImmutableMStreamTableCount
func (opt Options) WithMaxImmutableMStreamTableCount(val int) Options {
	opt.MaxImmutableMStreamTableCount = val
	return opt
}

//WithMaxWalSize
func (opt Options) WithMaxWalSize(val int64) Options {
	opt.MaxWalSize = val
	return opt
}

//EntryQueueCap
func (opt Options) WithEntryQueueCap(val int) Options {
	opt.EntryQueueCap = val
	return opt
}
