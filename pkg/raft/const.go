package etcdserver

const (

	// max number of in-flight snapshot messages etcdserver allows to have
	// This number is more than enough for most clusters with 5 machines.
	maxInFlightMsgSnap = 16
)

