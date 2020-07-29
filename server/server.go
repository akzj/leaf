package server

import "github.com/akzj/sstore"
import "github.com/gin-gonic/gin"

type Server struct {
	sstore *sstore.SStore
	engine *gin.Engine
}

func (server *Server) snapshot() {
	server.sstore.GetSnapshot()
}
