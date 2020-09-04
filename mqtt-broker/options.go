package mqtt_broker

type Options struct {
	MetaServerAddr             string `json:"meta_server_addr"`
	HOST                       string `json:"host"`
	BindPort                   int    `json:"bind_port"`
	BindTLSPort                int    `json:"bind_tls_port"`
	DefaultKeepalive           uint16 `json:"default_keepalive"`
	SubTreeCheckpointEventSize int64  `json:"tree_checkpoint_event_size"`
	SnapshotPath               string `json:"snapshot_path"`
	BrokerId                   int64  `json:"broker_id"`
}
