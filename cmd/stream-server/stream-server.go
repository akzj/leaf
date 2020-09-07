package main

import (
	server "github.com/akzj/streamIO/stream-server"
)

func main() {
	if err := server.New(server.DefaultOptions()).Start(); err != nil {
		panic(err)
	}
}
