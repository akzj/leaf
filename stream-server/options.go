package stream_server

import "time"

type Options struct {
	MetaServerAddr    string
	HeartbeatInterval time.Duration
}
