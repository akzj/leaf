package main

import (
	"context"
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
	"strings"
)

var ctx = context.Background()

func init() {
	log.SetOutput(os.Stderr)
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

func main() {
	app := cli.App{
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "ms",
				Aliases:  []string{"m"},
				Usage:    "meta-server-addr",
				Value:    "127.0.0.1:5000",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "stream-server",
				Aliases: []string{"s"},
				Usage:   "manager stream-server",
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
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(strings.Repeat("-", 100))
		fmt.Println(err.Error())
		fmt.Println(strings.Repeat("-", 100))
	}
}
