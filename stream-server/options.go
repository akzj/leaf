package stream_server

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type Options struct {
	MetaServerAddr        string        `json:"meta_server_addr"`
	ServerID              int64         `json:"server_id"`
	Host                  string        `json:"grpc_bind_addr"`
	GRPCPort              int           `json:"grpc_port"`
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
		GRPCPort:              7000,
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

func (option Options) WithGRPCPort(val int) Options {
	option.GRPCPort = val
	return option
}
