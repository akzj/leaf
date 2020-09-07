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
