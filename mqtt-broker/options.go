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

package mqtt_broker

import (
	"github.com/sirupsen/logrus"
	"time"
)

type Options struct {
	MetaServerAddr           string        `json:"meta_server_addr"`
	HOST                     string        `json:"host"`
	WSPort                   int           `json:"ws_port"`
	WSSPort                  int           `json:"wss_port"`
	BindPort                 int           `json:"bind_port"`
	BindTLSPort              int           `json:"bind_tls_port"`
	DefaultKeepalive         uint16        `json:"default_keepalive"`
	MinKeepalive             uint16        `json:"min_keepalive"`
	CheckpointEventSize      int64         `json:"checkpoint_event_size"`
	SnapshotPath             string        `json:"snapshot_path"`
	BrokerId                 int64         `json:"broker_id"`
	LogFile                  string        `json:"log_file"`
	LogLevel                 logrus.Level  `json:"log_level"`
	ReadOffsetCommitInterval time.Duration `json:"read_offset_commit_interval"`
	SysInterval              time.Duration `json:"sys_interval"`
}

func DefaultOptions() Options {
	return Options{
		MetaServerAddr:           "127.0.0.1:5000",
		HOST:                     "0.0.0.0",
		WSPort:                   10000,
		WSSPort:                  0,
		BindPort:                 12000,
		BindTLSPort:              0,
		DefaultKeepalive:         300,
		MinKeepalive:             60,
		CheckpointEventSize:      10,
		SnapshotPath:             "mqtt-broker-snapshot",
		BrokerId:                 8000,
		LogFile:                  "log/mqtt-broker.log",
		LogLevel:                 logrus.InfoLevel,
		ReadOffsetCommitInterval: time.Second,
		SysInterval:              time.Second * 5,
	}
}

func (options Options) WithMetaServerAddr(val string) Options {
	options.MetaServerAddr = val
	return options
}

func (options Options) WithHOST(val string) Options {
	options.HOST = val
	return options
}

func (options Options) WithBindPort(val int) Options {
	options.BindPort = val
	return options
}

func (options Options) WithBindTLSPort(val int) Options {
	options.BindTLSPort = val
	return options
}

func (options Options) WithDefaultKeepalive(val uint16) Options {
	options.DefaultKeepalive = val
	return options
}

func (options Options) WithCheckpointEventSize(val int64) Options {
	options.CheckpointEventSize = val
	return options
}

func (options Options) WithSnapshotPath(val string) Options {
	options.SnapshotPath = val
	return options
}
func (options Options) WithBrokerId(val int64) Options {
	options.BrokerId = val
	return options
}

func (options Options) WithLogFile(val string) Options {
	options.LogFile = val
	return options
}

func (options Options) WithSysInterval(val time.Duration) Options {
	options.SysInterval = val
	return options
}
