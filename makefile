

build:build-meta-server \
	build-stream-server \
	build-cli\
	build-example-writer\
	build-example-reader


build-example-reader:
	@/bin/echo "build example/reader"
	@go build -o bin/example/reader example/reader/reader.go

build-example-writer:
	@/bin/echo "build example/writer"
	@go build -o bin/example/writer example/writer/writer.go

build-cli:
	@/bin/echo "build cli"
	@go build -o bin/cli cmd/cli/cli.go
build-stream-server:
	@/bin/echo "build stream-server"
	@go build -o bin/stream-server cmd/stream-server/stream-server.go
build-meta-server:
	@/bin/echo "build meta-server"
	@go build -o bin/meta-server cmd/meta-server/meta-server.go

generate:
	@cd .. && protoc --go_out=plugins=grpc:. streamIO/proto/*.proto &&\
protoc --go_out=plugins=grpc:. streamIO/meta-server/store/*proto && \
protoc --go_out=plugins=grpc:. streamIO/mqtt-broker/*.proto && cd -
