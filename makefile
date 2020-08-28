

generate:
	@cd .. && protoc --go_out=plugins=grpc:. streamIO/proto/*.proto &&\
protoc --go_out=plugins=grpc:. streamIO/meta-server/store/*proto && cd -
