package main

import (
	broker "github.com/akzj/streamIO/mqtt-broker"
)

func main() {
	if err := broker.New(broker.DefaultOptions()).Start(); err != nil {
		panic(err)
	}
}
