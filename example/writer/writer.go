package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/akzj/streamIO/client"
	"time"
)

func main() {
	var metaServer string
	var data string
	flag.StringVar(&metaServer, "ms", "127.0.0.1:5000", "--ms [ip:port]")
	flag.StringVar(&data, "data", "hello"+time.Now().String(), "--data []")
	flag.Parse()

	ctx := context.Background()
	msClient, err := client.NewMetaServiceClient(ctx, metaServer)
	if err != nil {
		panic(err)
	}
	client := client.NewClient(msClient)

	infoItem, err := client.GetOrCreateStreamInfoItem(ctx, "hello")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(infoItem)

	session, err := client.NewStreamSession(ctx, 1, infoItem)
	if err != nil {
		panic(err.Error())
	}

	writer, err := session.NewWriter()
	if err != nil {
		panic(err.Error())
	}

	n, err := writer.Write([]byte("hello world"))
	if err != nil {
		panic(err.Error())
	}
	if err := writer.Flush(); err != nil {
		panic(err.Error())
	}
	fmt.Println(n)
}
