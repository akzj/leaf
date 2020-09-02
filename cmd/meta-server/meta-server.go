package main

import (
	"fmt"
	metaServer "github.com/akzj/streamIO/meta-server"
)

func main() {
	if err := metaServer.NewMetaServer(metaServer.DefaultOptions()).Start(); err != nil {
		fmt.Println(err.Error())
	}
}
