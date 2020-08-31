package meta_server

import (
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/sirupsen/logrus"
)

type MetaServer struct {
	log   *logrus.Logger
	store *store.Store
}
