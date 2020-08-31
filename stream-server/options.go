package stream_server

import "time"

type Options struct {
	MetaServerAddr    string        `json:"meta_server_addr"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	ServerID          uint32        `json:"server_id"`
	GRPCBindAddr      string        `json:"grpc_bind_addr"`
	StorePath         string        `json:"store_path"`
}
