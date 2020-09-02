module github.com/akzj/streamIO

go 1.13

require (
	github.com/akzj/block-queue v0.0.0-20200827035728-13cfec9cd1df
	github.com/akzj/mmdb v0.1.0
	github.com/akzj/sstore v0.1.1
	github.com/fatih/color v1.9.0
	github.com/golang/protobuf v1.4.2
	github.com/google/btree v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/rodaine/table v1.0.1
	github.com/sirupsen/logrus v1.6.0
	github.com/urfave/cli/v2 v2.2.0
	golang.org/x/net v0.0.0-20200822124328-c89045814202 // indirect
	golang.org/x/sys v0.0.0-20200826173525-f9321e4c35a6 // indirect
	golang.org/x/text v0.3.3 // indirect
	google.golang.org/genproto v0.0.0-20200827165113-ac2560b5e952 // indirect
	google.golang.org/grpc v1.31.1
	google.golang.org/protobuf v1.25.0
	gopkg.in/yaml.v2 v2.2.8 // indirect
)

replace (
	github.com/akzj/mmdb v0.1.0 => ../mmdb
	github.com/akzj/sstore v0.1.1 => ../sstore
)
