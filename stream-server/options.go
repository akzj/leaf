package stream_server

import "time"

type Options struct {
	MetaServerAddr    string        `json:"meta_server_addr"`
	ServerID          int64         `json:"server_id"`
	GRPCBindAddr      string        `json:"grpc_bind_addr"`
	SStorePath        string        `json:"store_path"`
	LogPath           string        `json:"log_path"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
}

func DefaultOptions() Options {
	return Options{
		MetaServerAddr:    "127.0.0.1:5000",
		ServerID:          1,
		GRPCBindAddr:      "127.0.0.1:7000",
		SStorePath:        "sstore",
		LogPath:           "log/stream-server.log",
		HeartbeatInterval: time.Second,
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

func (option Options) WithGRPCBindAddr(val string) Options {
	option.GRPCBindAddr = val
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
