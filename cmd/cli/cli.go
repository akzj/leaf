package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/akzj/streamIO/client"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/fatih/color"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/rodaine/table"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"os"
	"strconv"
	"strings"
	"time"
)

var ctx = context.Background()

func init() {
	log.SetOutput(os.Stderr)
	log.SetReportCaller(true)
}
func addStreamServer(metaServer string, id int64, addr string) error {
	client, err := client.NewMetaServiceClient(ctx, metaServer)
	if err != nil {
		fmt.Println(err.Error())
	}
	_, err = client.AddStreamServer(ctx,
		&proto.AddStreamServerRequest{
			StreamServerInfoItem: &store.StreamServerInfoItem{
				Base: &store.ServerInfoBase{
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

	if _, err := client.GetOrCreateStream(ctx, streamName); err != nil {
		log.Error(err.Error())
		return err
	}

	session, err := client.NewStreamSession(ctx, 1, streamName)
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
		fmt.Println(writer.Close())
	}()
	var size int
	hash := md5.New()
	for count := count; count > 0; count-- {
		buffer := data + strconv.Itoa(int(count)) + "\n"
		n, err := writer.Write([]byte(buffer))
		if err != nil {
			panic(err.Error())
		}
		if n != len(buffer) {
			panic("write error")
		}
		if err := writer.Flush(); err != nil {
			log.Error(err.Error())
		}
		size += n
		hash.Write([]byte(buffer))
	}
	if err := writer.Flush(); err != nil {
		log.Error(err)
		return nil
	}
	fmt.Printf("write stream [%s] count [%d]  size [%d] md5[%x]\n", streamName, count, size, hash.Sum(nil))
	return nil
}

func getStreamStat(metaServer string, streamName string) error {
	fmt.Println(metaServer, streamName)
	msClient, err := client.NewMetaServiceClient(ctx, metaServer)
	if err != nil {
		panic(err)
	}
	client := client.NewClient(msClient)
	defer func() {
		client.Close()
	}()
	id, err := client.GetStreamID(ctx, streamName)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	begin, end, err := client.GetStreamStat(ctx, streamName)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Println("id", id, "begin", begin, "end", end)
	return nil
}

func readStream(metaServer string, streamName string, size int64, reset bool) error {
	fmt.Println(metaServer, streamName, size, reset)
	msClient, err := client.NewMetaServiceClient(ctx, metaServer)
	if err != nil {
		panic(err)
	}
	client := client.NewClient(msClient)
	defer func() {
		client.Close()
	}()

	if id, err := client.GetOrCreateStream(ctx, streamName); err != nil {
		log.Error(err.Error())
		return err
	} else {
		fmt.Println(streamName, id)
	}

	session, err := client.NewStreamSession(ctx, 8000, streamName)
	if err != nil {
		log.Error(err)
		return nil
	}
	if reset {
		_, end, err := client.GetStreamStat(ctx, streamName)
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
		fmt.Println(string(message.Payload()))
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

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	time.Sleep(time.Second)

	for i := 0; i < count; i++ {
		text := fmt.Sprintf("this is msg #%d!", i)
		token := c.Publish(topic, 0, false, text)
		token.Wait()
		if token.Error() != nil {
			return token.Error()
		}
	}
	c.Disconnect(250)
	return nil
}

func main() {
	app := cli.App{
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "ms",
				Aliases: []string{"m"},
				Usage:   "meta-server-addr",
				Value:   "127.0.0.1:5000",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "stream-server",
				Aliases: []string{"s"},
				Usage:   "stream-server operator",
				Subcommands: []*cli.Command{
					{
						Name: "list",
						Action: func(c *cli.Context) error {
							return listStreamServer(c.String("ms"))
						},
					},
					{
						Name:    "add",
						Aliases: []string{"a"},
						Usage:   "add stream-server",
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
							metaServerAddr := c.String("ms")
							id := c.Int64("id")
							addr := c.String("addr")
							return addStreamServer(metaServerAddr, id, addr)
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
						Usage: "--broker [broker addr],eg : --broker tcp://127.0.0.1:8000",
						Value: "tcp://127.0.0.1:8000",
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
								Value: "test-stream-02",
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
							return writeStream(c.String("ms"),
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
								Value:    "test-stream-02",
							},
							&cli.Int64Flag{
								Name:  "size",
								Usage: "read size",
							},
							&cli.BoolFlag{
								Name:  "reset",
								Usage: "reset read offset to end",
								Value: false,
							},
						},
						Action: func(c *cli.Context) error {
							return readStream(c.String("ms"),
								c.String("name"),
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
							return getStreamStat(c.String("ms"), c.String("name"))
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
