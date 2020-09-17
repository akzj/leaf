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

package stream_server

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type Options struct {
	MetaServerAddr        string        `json:"meta_server_addr"`
	ServerID              int64         `json:"server_id"`
	Host                  string        `json:"host"`
	StreamPort            int           `json:"stream_port"`
	SyncPort              int           `json:"sync_port"`
	SyncFrom              string        `json:"sync_from"`
	SStorePath            string        `json:"store_path"`
	LogPath               string        `json:"log_path"`
	AutoAddServer         bool          `json:"auto_add_server"`
	LogLevel              log.Level     `json:"log_level"`
	HeartbeatInterval     time.Duration `json:"heartbeat_interval"`
	DialMetaServerTimeout time.Duration `json:"dial_meta_server_timeout"`
}

func DefaultOptions() Options {
	return Options{
		MetaServerAddr:        "127.0.0.1:5000",
		ServerID:              1,
		Host:                  "127.0.0.1",
		StreamPort:            7000,
		SyncPort:              9000,
		SyncFrom:              "",
		SStorePath:            "sstore",
		LogPath:               "log/stream-server.log",
		AutoAddServer:         true,
		LogLevel:              log.InfoLevel,
		HeartbeatInterval:     time.Second,
		DialMetaServerTimeout: time.Second * 30,
	}
}

func (option Options) WithMetaServerAddr(val string) Options {
	option.MetaServerAddr = val
	return option
}

func (option Options) WithServerID(val int64) Options {
	option.ServerID = val
	return option
}

func (option Options) WithHost(val string) Options {
	option.Host = val
	return option
}

func (option Options) WithSStorePath(val string) Options {
	option.SStorePath = val
	return option
}

func (option Options) WithLogPath(val string) Options {
	option.LogPath = val
	return option
}

func (option Options) WithHeartbeatInterval(val time.Duration) Options {
	option.HeartbeatInterval = val
	return option
}

func (option Options) WithAutoAddServer(val bool) Options {
	option.AutoAddServer = val
	return option
}

func (option Options) WithDialMetaServerTimeout(val time.Duration) Options {
	option.DialMetaServerTimeout = val
	return option
}

func (option Options) WithStreamPort(val int) Options {
	option.StreamPort = val
	return option
}

func (option Options) WithSyncPort(val int) Options {
	option.SyncPort = val
	return option
}
