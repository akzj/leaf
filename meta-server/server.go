package meta_server

import (
	"context"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	"github.com/sirupsen/logrus"
)

type MetaServer struct {
	log   *logrus.Logger
	store *store.Store
}


