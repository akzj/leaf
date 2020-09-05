package mqtt_broker

type Options struct {
	MetaServerAddr      string `json:"meta_server_addr"`
	HOST                string `json:"host"`
	BindPort            int    `json:"bind_port"`
	BindTLSPort         int    `json:"bind_tls_port"`
	DefaultKeepalive    uint16 `json:"default_keepalive"`
	CheckpointEventSize int64  `json:"checkpoint_event_size"`
	SnapshotPath        string `json:"snapshot_path"`
	BrokerId            int64  `json:"broker_id"`
}

func DefaultOptions() Options {
	return Options{
		MetaServerAddr:      "127.0.0.1:5000",
		HOST:                "0.0.0.0",
		BindPort:            8000,
		BindTLSPort:         0,
		DefaultKeepalive:    300,
		CheckpointEventSize: 100,
		SnapshotPath:        "mqtt-broker-snapshot",
		BrokerId:            8000,
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