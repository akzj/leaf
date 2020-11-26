// Copyright 2020-2026 The streamIO Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/akzj/streamIO/client"
	"github.com/akzj/streamIO/proto"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/fatih/color"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/rodaine/table"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ctx = context.Background()

func init() {
	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.TextFormatter{DisableQuote: true})
}
func addStreamServer(metaServer string, id int64, addr string) error {
	client, err := client.NewMetaServiceClient(ctx, metaServer)
	if err != nil {
		fmt.Println(err.Error())
	}
	_, err = client.AddStreamServer(ctx,
		&proto.AddStreamServerRequest{
			StreamServerInfoItem: &proto.StreamServerInfoItem{
				Base: &proto.ServerInfoBase{
					Id:   id,
					Addr: addr},
			},
		},
	)
	if err != nil {
		log.WithField("metasServer", metaServer).
			WithField("id", id).
			WithField("addr", addr).
			Error(err)
	}
	return nil
}

func listStreamServer(metaServer string) error {
	client, err := client.NewMetaServiceClient(ctx, metaServer)
	if err != nil {
		fmt.Println(err.Error())
	}
	response, err := client.ListStreamServer(ctx, &empty.Empty{})
	if err != nil {
		log.WithField("metasServer", metaServer).Error(err)
		return nil
	}

	headerFmt := color.New(color.FgGreen, color.Underline).SprintfFunc()
	columnFmt := color.New(color.FgYellow).SprintfFunc()

	tbl := table.New("id", "addr", "leader")
	tbl.WithHeaderFormatter(headerFmt).WithFirstColumnFormatter(columnFmt)

	for _, it := range response.Items {
		tbl.AddRow(it.Base.Id, it.Base.Addr, it.Base.Leader)
	}
	tbl.Print()
	return nil
}

func writeStream(metaServer string, streamName string, data string, count int64) error {
	fmt.Println(metaServer, streamName, data, count)
	msClient, err := client.NewMetaServiceClient(ctx, metaServer)
	if err != nil {
		panic(err)
	}
	client := client.NewClient(msClient)
	defer func() {
		client.Close()
	}()

	info, err := client.GetOrCreateStreamInfoItem(ctx, streamName)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	session, err := client.NewStreamSession(ctx, 1, info)
	if err != nil {
		log.Error(err)
		return nil
	}

	writer, err := session.NewWriter()
	if err != nil {
		log.Error(err)
		return nil
	}
	go func() {
		time.Sleep(time.Second * 3)
	}()
	var size int
	hash := md5.New()
	var wg sync.WaitGroup
	for i := int64(1); i <= count; i++ {
		wg.Add(1)
		buffer := data + strconv.Itoa(int(i)) + "\n"
		writer.WriteWithCb([]byte(buffer), func(err error) {
			wg.Done()
			if err != nil {
				panic(err.Error())
			}
		})
		hash.Write([]byte(buffer))
	}

	wg.Wait()

	fmt.Printf("write stream [%s] count [%d]  size [%d] md5[%x]\n", streamName, count, size, hash.Sum(nil))
	return nil
}

func getStreamStat(metaServer string, streamName string) error {
	fmt.Println(metaServer, streamName)
	msClient, err := client.NewMetaServiceClient(ctx, metaServer)
	if err != nil {
		panic(err)
	}
	cc := client.NewClient(msClient)
	defer func() {
		_ = cc.Close()
	}()
	streamInfoItem, err := cc.GetStreamInfoItem(ctx, streamName)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	begin, end, err := cc.GetStreamStat(ctx, streamInfoItem)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Println("id", streamInfoItem, "begin", begin, "end", end)
	return nil
}

func readStream(metaServer string, streamName string, sessionID int64, size int64, reset bool) error {
	fmt.Println(metaServer, streamName, size, reset)
	msClient, err := client.NewMetaServiceClient(ctx, metaServer)
	if err != nil {
		panic(err)
	}
	cc := client.NewClient(msClient)
	defer func() {
		_ = cc.Close()
	}()

	infoItem, err := cc.GetOrCreateStreamInfoItem(ctx, streamName)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	session, err := cc.NewStreamSession(ctx, sessionID, infoItem)
	if err != nil {
		log.Error(err)
		return nil
	}
	if reset {
		_, end, err := cc.GetStreamStat(ctx, infoItem)
		if err != nil {
			log.Errorf("%+v", err)
			return nil
		}
		fmt.Println(end)
		if err := session.SetReadOffset(end); err != nil {
			log.Error(err)
			return nil
		}
	}
	reader, err := session.NewReader()
	if err != nil {
		log.Error(err)
		return nil
	}
	hash := md5.New()

	fmt.Println("reader offset", reader.Offset())

	for toRead := size; toRead > 0; {
		var data []byte
		if toRead > 1024 {
			data = make([]byte, 1024)
		} else {
			data = make([]byte, toRead)
		}
		n, err := reader.Read(data)
		if err != nil {
			log.Error(err)
			return err
		}
		data = data[:n]
		hash.Write(data)
		//fmt.Print(string(data))
		toRead -= int64(n)
	}
	if err := session.SetReadOffset(reader.Offset()); err != nil {
		log.Error(err.Error())
		return nil
	}
	fmt.Printf("read stream [%s] size [%d] md5[%x]\n", streamName, size, hash.Sum(nil))
	return nil
}

type mqttLogger struct {
}

func (m *mqttLogger) Println(v ...interface{}) {
	fmt.Println(fmt.Sprintf("%+v", v))
}

func (m *mqttLogger) Printf(format string, v ...interface{}) {
	fmt.Println(fmt.Sprintf(format, v))
}

func mqttSub(brokerAddr string, topic string, clientID string) error {
	fmt.Println(brokerAddr, topic, clientID)

	mqtt.ERROR = &mqttLogger{}
	opts := mqtt.NewClientOptions().AddBroker(brokerAddr).SetClientID(clientID)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetCleanSession(false)
	opts.SetDefaultPublishHandler(func(c mqtt.Client, message mqtt.Message) {
		fmt.Println(message.Topic(), string(message.Payload()))
	})

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	if token := c.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	select {}
}

func mqttPub(brokerAddr string, topic string, clientID string, count int) error {
	fmt.Println(brokerAddr, topic, clientID, count)
	mqtt.ERROR = &mqttLogger{}
	opts := mqtt.NewClientOptions().AddBroker(brokerAddr).SetClientID(clientID)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetCleanSession(false)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	time.Sleep(time.Second)

	for i := 0; i < count; i++ {
		text := fmt.Sprintf("this is msg #%d!", i)
		token := c.Publish(topic, 0, true, text)
		token.Wait()
		if token.Error() != nil {
			return token.Error()
		}
	}
	c.Disconnect(250)
	return nil
}

func getStreamServerStoreVersion(c *cli.Context) error {
	metaServer := c.String("addr")
	conn, err := grpc.DialContext(ctx, metaServer, grpc.WithInsecure())
	if err != nil {
		return err
	}
	client := proto.NewStreamServiceClient(conn)

	version, err := client.GetStreamStoreVersion(ctx, &proto.GetStreamStoreVersionRequest{})
	if err != nil {
		return err
	}
	fmt.Println("index", version.Index)
	return nil
}

func main() {
	app := cli.App{
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "mss",
				Aliases: []string{"m"},
				Usage:   "meta-server-addr",
				Value:   "127.0.0.1:5000",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "meta-server",
				Aliases: []string{"ms"},
				Usage:   "meta-server api",
				Subcommands: []*cli.Command{
					{
						Name:    "list_stream_server",
						Usage:   "list stream-server info registering in meta-server",
						Aliases: []string{"lss"},
						Action: func(c *cli.Context) error {
							return listStreamServer(c.String("mss"))
						},
					},
					{
						Name:    "add_stream_server",
						Aliases: []string{"ass"},
						Usage:   "register stream-server info to meta-server store",
						Flags: []cli.Flag{
							&cli.Int64Flag{
								Name:     "id",
								Aliases:  []string{"i"},
								Usage:    "stream-server uniq id",
								Required: true,
							},
							&cli.StringFlag{
								Name:     "addr",
								Aliases:  []string{"a"},
								Usage:    "stream-server addr",
								Required: true,
							},
						},
						Action: func(c *cli.Context) error {
							metaServerAddr := c.String("mss")
							id := c.Int64("id")
							addr := c.String("addr")
							return addStreamServer(metaServerAddr, id, addr)
						},
					},
				},
			},
			{
				Name:  "stream-server",
				Usage: "stream-server api",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "addr",
						Usage: "stream-server addr",
						Value: "127.0.0.1:7000",
					},
				},
				Subcommands: []*cli.Command{
					{
						Name:    "version",
						Usage:   "get version of stream-server store",
						Aliases: []string{"ver"},
						Action: func(c *cli.Context) error {
							return getStreamServerStoreVersion(c)
						},
					},
				},
			},
			{
				Name:  "mqtt",
				Usage: "mqtt test",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "broker",
						Usage: "--broker [broker addr],eg : --broker tcp://127.0.0.1:12000",
						Value: "tcp://127.0.0.1:12000",
					},
					&cli.StringFlag{
						Name:  "topic",
						Usage: "topic to subscribe",
						Value: "streamIO/mqtt_broker/test",
					},
					&cli.IntFlag{
						Name:  "count",
						Usage: "the count of message to pub",
						Value: 10000,
					},
				},
				Subcommands: []*cli.Command{
					{
						Flags: []cli.Flag{&cli.StringFlag{
							Name:  "id",
							Usage: "client identifier uniq ID",
							Value: "streamIO_cli_pub_test",
						}},
						Name: "pub",
						Action: func(c *cli.Context) error {
							return mqttPub(c.String("broker"), c.String("topic"), c.String("id"), c.Int("count"))
						},
					},
					{
						Flags: []cli.Flag{&cli.StringFlag{
							Name:  "id",
							Usage: "client identifier uniq ID",
							Value: "streamIO_cli_sub_test",
						}},
						Name: "sub",
						Action: func(c *cli.Context) error {
							return mqttSub(c.String("broker"), c.String("topic"), c.String("id"))
						},
					},
				},
			},
			{
				Name:  "test",
				Usage: "test tools",
				Subcommands: []*cli.Command{
					{
						Name: "write",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:  "name",
								Usage: "stream name",
								Value: "cli-test",
							},
							&cli.Int64Flag{
								Name:  "count",
								Usage: "write count",
								Value: 1,
							},
							&cli.StringFlag{
								Name:  "data",
								Usage: "data to write",
								Value: "h",
							},
						},
						Action: func(c *cli.Context) error {
							return writeStream(c.String("mss"),
								c.String("name"),
								c.String("data"),
								c.Int64("count"))
						},
					},
					{
						Name: "read",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "name",
								Usage:    "stream name",
								Required: false,
								Value:    "cli-test",
							},
							&cli.Int64Flag{
								Name:  "size",
								Usage: "read size",
							},
							&cli.Int64Flag{
								Name:  "session_id",
								Usage: "stream session id",
							},
							&cli.BoolFlag{
								Name:  "reset",
								Usage: "reset read offset to end",
								Value: false,
							},
						},
						Action: func(c *cli.Context) error {
							return readStream(c.String("mss"),
								c.String("name"),
								c.Int64("session_id"),
								c.Int64("size"),
								c.Bool("reset"))
						},
					},
					{
						Name: "stat",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "name",
								Usage:    "get streamID status",
								Required: true,
							},
						},
						Action: func(c *cli.Context) error {
							return getStreamStat(c.String("mss"), c.String("name"))
						},
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(strings.Repeat("-", 100))
		fmt.Println(err.Error())
		fmt.Println(strings.Repeat("-", 100))
	}
}
