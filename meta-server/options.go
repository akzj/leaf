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

package meta_server

import (
	"github.com/akzj/streamIO/pkg/mmdb"
	"github.com/sirupsen/logrus"
)

type Options struct {
	LogFile     string       `json:"log"`
	LogLevel    logrus.Level `json:"log_level"`
	GRPCBind    int          `json:"grpc_bind"`
	MMdbOptions mmdb.Options `json:"mmdb_options"`
}

func DefaultOptions() Options {
	return Options{
		LogFile:  "log/meta-server.log",
		LogLevel: logrus.InfoLevel,
		GRPCBind: 5000,
		MMdbOptions: mmdb.DefaultOptions().
			WithSyncWrite(false).
			WithRecovery(true),
	}
}

func (option Options) WithLogFile(logFile string) Options {
	option.LogFile = logFile
	return option
}

func (option Options) WithLogLevel(LogLevel logrus.Level) Options {
	option.LogLevel = LogLevel
	return option
}

func (option Options) WithGRPCBind(port int) Options {
	option.GRPCBind = port
	return option
}

func (option Options) WithMMdbOptions(options mmdb.Options) Options {
	option.MMdbOptions = options
	return option
}

func (option Options) GetMMdbOptions() mmdb.Options {
	return option.MMdbOptions
}
