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
	server "github.com/akzj/streamIO/stream-server"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"os"
	"strconv"
	"time"
)

func startStreamServer(ctx *cli.Context) error {
	options := server.Options{
		MetaServerAddr:        ctx.String("meta_server_addr"),
		ServerID:              ctx.Int64("server_id"),
		Host:                  ctx.String("host"),
		StreamPort:            ctx.Int("stream_port"),
		SyncPort:              ctx.Int("sync_port"),
		SyncFrom:              ctx.String("sync_from"),
		SStorePath:            ctx.String("store_path"),
		LogPath:               ctx.String("log_path"),
		AutoAddServer:         ctx.Bool("auto_add_server"),
		LogLevel:              log.Level(ctx.Int("log_level")),
		HeartbeatInterval:     ctx.Duration("heartbeat_interval"),
		DialMetaServerTimeout: ctx.Duration("dial_meta_server_timeout"),
	}
	if err := server.New(options).Start(); err != nil {
		panic(err)
		return nil
	}
	return nil
}

func main() {

	opts := server.DefaultOptions()
	app := cli.App{
		Name:  "stream-server",
		Usage: "service for brokers [mqtt-broker,amqp-broker,stomp-broker] reading/writing message",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "meta_server_addr",
				Aliases:     []string{"ms"},
				Usage:       "meta-server addr",
				Value:       opts.MetaServerAddr,
				DefaultText: opts.MetaServerAddr,
			},
			&cli.Int64Flag{
				Name:        "server_id",
				Usage:       "stream-server uniq id",
				Value:       opts.ServerID,
				DefaultText: strconv.FormatInt(opts.ServerID, 10),
			},
			&cli.StringFlag{
				Name:        "host",
				Usage:       "host to bind",
				Value:       opts.Host,
				DefaultText: opts.Host,
			},
			&cli.IntFlag{
				Name:        "stream_port",
				Usage:       "stream service port",
				Value:       opts.StreamPort,
				DefaultText: strconv.Itoa(opts.StreamPort),
			},
			&cli.IntFlag{
				Name:        "sync_port",
				Usage:       "use for supporting `master/slave` replication data",
				Value:       opts.SyncPort,
				DefaultText: strconv.Itoa(opts.SyncPort),
			},
			&cli.StringFlag{
				Name:        "sync_from",
				Aliases:     []string{"sf"},
				Usage:       "sync data from master stream-server",
				Value:       opts.SyncFrom,
				DefaultText: opts.SyncFrom,
			},
			&cli.StringFlag{
				Name:        "store_path",
				Aliases:     []string{"sp"},
				Usage:       "path for storing stream data",
				Value:       opts.SStorePath,
				DefaultText: opts.SStorePath,
			},
			&cli.StringFlag{
				Name:        "log_path",
				Aliases:     []string{"lp"},
				Usage:       "stream-server log path",
				Value:       opts.LogPath,
				DefaultText: opts.LogPath,
			},
			&cli.BoolFlag{
				Name:        "auto_add_server",
				Aliases:     []string{"aas"},
				Usage:       "auto add stream-server to meta-server,using for debugging",
				Value:       true,
				DefaultText: "true",
			},
			&cli.IntFlag{
				Name:        "log_level",
				Usage:       `log level {0:"PanicLevel",1:"FatalLevel",2:"ErrorLevel",3:"WarnLevel",4:"InfoLevel",5:"DebugLevel",6:"TraceLevel"}`,
				Value:       int(opts.LogLevel),
				DefaultText: strconv.Itoa(int(opts.LogLevel)),
			},
			&cli.DurationFlag{
				Name:        "heartbeat_interval",
				Usage:       "send heartbeat message to meta-server interval",
				Value:       opts.HeartbeatInterval,
				DefaultText: opts.HeartbeatInterval.String(),
			},
			&cli.DurationFlag{
				Name:        "dial_meta_server_timeout",
				Usage:       "dial meta-server timeout",
				Value:       opts.DialMetaServerTimeout,
				DefaultText: opts.DialMetaServerTimeout.String(),
			},
		},
		HideHelpCommand: true,
		Action: func(c *cli.Context) error {
			return startStreamServer(c)
		},
		Compiled:  time.Now(),
		Copyright: "Copyright 2020-2026 The streamIO Authors",
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
