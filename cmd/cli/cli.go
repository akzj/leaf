package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/akzj/streamIO/client"
	"github.com/akzj/streamIO/meta-server/store"
	"github.com/akzj/streamIO/proto"
	"github.com/fatih/color"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/rodaine/table"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"os"
	"strconv"
	"strings"
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
	defer writer.Close()
	var size int
	hash := md5.New()
	for count := count; count > 0; count-- {
		buffer := string(data) + strconv.Itoa(int(count)) + "\n"
		n, err := writer.Write([]byte(buffer))
		if err != nil {
			panic(err.Error())
		}
		if n != len(buffer) {
			panic("write error")
		}
		size += n
		hash.Write([]byte(buffer))
	}
	fmt.Printf("write stream [%s] count [%d]  size [%d] md5[%x]\n", streamName, count, size, hash.Sum(nil))
	return nil
}

func readStream(metaServer string, streamName string, size int64) error {
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

	reader, err := session.NewReader()
	if err != nil {
		log.Error(err)
		return nil
	}
	hash := md5.New()

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
			return nil
		}
		data = data[:n]
		hash.Write(data)
		//fmt.Print(string(data))
		toRead -= int64(n)
	}
	fmt.Printf("read stream [%s] size [%d] md5[%x]\n", streamName, size, hash.Sum(nil))
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
								Value: 1024,
							},
							&cli.StringFlag{
								Name:  "data",
								Usage: "data to write",
								Value: "hello world",
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
								Name:     "size",
								Usage:    "read size",
								Required: false,
							},
						},
						Action: func(c *cli.Context) error {
							return readStream(c.String("ms"), c.String("name"), c.Int64("size"))
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
