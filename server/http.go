package server

import (
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"io"
)

type jsonRender struct {
}

func (server *Server) handleReadStreamRequest(request readStreamRequest, c *gin.Context) error {
	begin, ok := server.sstore.Begin(request.Name)
	if ok == false {
		c.JSON(200, gin.H{"code": 0, "message": ErrNoFindStream})
		return ErrNoFindStream
	}
	end, ok := server.sstore.End(request.Name)
	if !ok {
		panic("get end of stream error")
	}
	if request.Begin > 0 {
		if begin > request.Begin {
			c.JSON(200, gin.H{"code": 0, "message": ErrNoFindStream})
			return ErrNoFindStream
		}
	}
	if request.Begin == 0 {
		request.Begin = begin
	}
	if request.End > 0 {
		if end < request.End {
			c.JSON(200, gin.H{"code": 0, "message": ErrInvalidStreamBegin})
			return ErrInvalidStreamBegin
		}
	}
	reader, err := server.sstore.Reader(request.Name)
	if err != nil {
		panic(err)
	}
	if _, err := reader.Seek(request.Begin, io.SeekStart); err != nil {
		panic(err)
	}
	if request.End > 0 {
		if _, err := io.CopyN(c.Writer, reader, request.End-request.Begin); err != nil {
			//todo log err
		}
	} else {
		if _, err := io.CopyN(c.Writer, reader, end-request.Begin); err != nil {
			return errors.Wrap(err, "read stream failed")
		}
		request.Begin = end
		watcher := server.sstore.Watcher(request.Name)
		defer watcher.Close()
		for {
			select {
			case <-watcher.Watch():

			case <-c.Done():
				return nil
			}
		}
	}
}

func (server *Server) initHttpHandler() {
	v1 := server.engine.Group("/v1")

	v1.GET("/stream/:name/:begin/:end", func(c *gin.Context) {
		var request readStreamRequest
		if err := c.BindUri(&request); err != nil {
			return
		}
		server.handleReadStreamRequest(request, c)
	})
}
