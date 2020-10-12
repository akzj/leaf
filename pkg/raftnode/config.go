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
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config holds the configuration of etcd as taken from the command line or discovery.
type Config struct {
	Name    string
	DataDir string

	ElectionTicks int

	// PreVote is true to enable Raft Pre-Vote.
	PreVote bool

	// Logger logs server-side operations.
	// If not nil, it disables "capnslog" and uses the given logger.
	Logger *zap.Logger

	// LoggerConfig is server logger configuration for Raft logger.
	// Must be either: "LoggerConfig != nil" or "LoggerCore != nil && LoggerWriteSyncer != nil".
	LoggerConfig *zap.Config
	// LoggerCore is "zapcore.Core" for raft logger.
	// Must be either: "LoggerConfig != nil" or "LoggerCore != nil && LoggerWriteSyncer != nil".
	LoggerCore        zapcore.Core
	LoggerWriteSyncer zapcore.WriteSyncer
}

func (c *Config) MemberDir() string { return filepath.Join(c.DataDir, "member") }

func (c *Config) WALDir() string {
	return filepath.Join(c.MemberDir(), "wal")
}
