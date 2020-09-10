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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.6.1
// source: streamIO/proto/stream.proto

package proto

import (
	context "context"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type WriteStreamRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamId  int64  `protobuf:"varint,1,opt,name=stream_id,json=streamId,proto3" json:"stream_id,omitempty"`
	Offset    int64  `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	Data      []byte `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
	RequestId int64  `protobuf:"varint,4,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
}

func (x *WriteStreamRequest) Reset() {
	*x = WriteStreamRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_proto_stream_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteStreamRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteStreamRequest) ProtoMessage() {}

func (x *WriteStreamRequest) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_proto_stream_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteStreamRequest.ProtoReflect.Descriptor instead.
func (*WriteStreamRequest) Descriptor() ([]byte, []int) {
	return file_streamIO_proto_stream_proto_rawDescGZIP(), []int{0}
}

func (x *WriteStreamRequest) GetStreamId() int64 {
	if x != nil {
		return x.StreamId
	}
	return 0
}

func (x *WriteStreamRequest) GetOffset() int64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *WriteStreamRequest) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *WriteStreamRequest) GetRequestId() int64 {
	if x != nil {
		return x.RequestId
	}
	return 0
}

type WriteStreamResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamId  int64  `protobuf:"varint,1,opt,name=stream_id,json=streamId,proto3" json:"stream_id,omitempty"`
	Offset    int64  `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	RequestId int64  `protobuf:"varint,3,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	Err       string `protobuf:"bytes,4,opt,name=err,proto3" json:"err,omitempty"`
}

func (x *WriteStreamResponse) Reset() {
	*x = WriteStreamResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_proto_stream_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteStreamResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteStreamResponse) ProtoMessage() {}

func (x *WriteStreamResponse) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_proto_stream_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteStreamResponse.ProtoReflect.Descriptor instead.
func (*WriteStreamResponse) Descriptor() ([]byte, []int) {
	return file_streamIO_proto_stream_proto_rawDescGZIP(), []int{1}
}

func (x *WriteStreamResponse) GetStreamId() int64 {
	if x != nil {
		return x.StreamId
	}
	return 0
}

func (x *WriteStreamResponse) GetOffset() int64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *WriteStreamResponse) GetRequestId() int64 {
	if x != nil {
		return x.RequestId
	}
	return 0
}

func (x *WriteStreamResponse) GetErr() string {
	if x != nil {
		return x.Err
	}
	return ""
}

type ReadStreamRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamId int64 `protobuf:"varint,1,opt,name=stream_id,json=streamId,proto3" json:"stream_id,omitempty"`
	Offset   int64 `protobuf:"varint,3,opt,name=offset,proto3" json:"offset,omitempty"`
	Size     int64 `protobuf:"varint,4,opt,name=size,proto3" json:"size,omitempty"`
	//add watcher to stream-server when stream no exist
	Watch bool `protobuf:"varint,5,opt,name=watch,proto3" json:"watch,omitempty"`
}

func (x *ReadStreamRequest) Reset() {
	*x = ReadStreamRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_proto_stream_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadStreamRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadStreamRequest) ProtoMessage() {}

func (x *ReadStreamRequest) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_proto_stream_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadStreamRequest.ProtoReflect.Descriptor instead.
func (*ReadStreamRequest) Descriptor() ([]byte, []int) {
	return file_streamIO_proto_stream_proto_rawDescGZIP(), []int{2}
}

func (x *ReadStreamRequest) GetStreamId() int64 {
	if x != nil {
		return x.StreamId
	}
	return 0
}

func (x *ReadStreamRequest) GetOffset() int64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *ReadStreamRequest) GetSize() int64 {
	if x != nil {
		return x.Size
	}
	return 0
}

func (x *ReadStreamRequest) GetWatch() bool {
	if x != nil {
		return x.Watch
	}
	return false
}

type ReadStreamResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Offset int64  `protobuf:"varint,3,opt,name=offset,proto3" json:"offset,omitempty"`
	Data   []byte `protobuf:"bytes,4,opt,name=Data,proto3" json:"Data,omitempty"`
}

func (x *ReadStreamResponse) Reset() {
	*x = ReadStreamResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_proto_stream_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadStreamResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadStreamResponse) ProtoMessage() {}

func (x *ReadStreamResponse) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_proto_stream_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadStreamResponse.ProtoReflect.Descriptor instead.
func (*ReadStreamResponse) Descriptor() ([]byte, []int) {
	return file_streamIO_proto_stream_proto_rawDescGZIP(), []int{3}
}

func (x *ReadStreamResponse) GetOffset() int64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *ReadStreamResponse) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type GetStreamStatRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamID int64 `protobuf:"varint,1,opt,name=streamID,proto3" json:"streamID,omitempty"`
}

func (x *GetStreamStatRequest) Reset() {
	*x = GetStreamStatRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_proto_stream_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetStreamStatRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetStreamStatRequest) ProtoMessage() {}

func (x *GetStreamStatRequest) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_proto_stream_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetStreamStatRequest.ProtoReflect.Descriptor instead.
func (*GetStreamStatRequest) Descriptor() ([]byte, []int) {
	return file_streamIO_proto_stream_proto_rawDescGZIP(), []int{4}
}

func (x *GetStreamStatRequest) GetStreamID() int64 {
	if x != nil {
		return x.StreamID
	}
	return 0
}

type GetStreamStatResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamID int64 `protobuf:"varint,1,opt,name=streamID,proto3" json:"streamID,omitempty"`
	Begin    int64 `protobuf:"varint,2,opt,name=begin,proto3" json:"begin,omitempty"`
	End      int64 `protobuf:"varint,3,opt,name=end,proto3" json:"end,omitempty"`
}

func (x *GetStreamStatResponse) Reset() {
	*x = GetStreamStatResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_proto_stream_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetStreamStatResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetStreamStatResponse) ProtoMessage() {}

func (x *GetStreamStatResponse) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_proto_stream_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetStreamStatResponse.ProtoReflect.Descriptor instead.
func (*GetStreamStatResponse) Descriptor() ([]byte, []int) {
	return file_streamIO_proto_stream_proto_rawDescGZIP(), []int{5}
}

func (x *GetStreamStatResponse) GetStreamID() int64 {
	if x != nil {
		return x.StreamID
	}
	return 0
}

func (x *GetStreamStatResponse) GetBegin() int64 {
	if x != nil {
		return x.Begin
	}
	return 0
}

func (x *GetStreamStatResponse) GetEnd() int64 {
	if x != nil {
		return x.End
	}
	return 0
}

var File_streamIO_proto_stream_proto protoreflect.FileDescriptor

var file_streamIO_proto_stream_proto_rawDesc = []byte{
	0x0a, 0x1b, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x4f, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2f, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0x7c, 0x0a, 0x12, 0x57, 0x72, 0x69, 0x74, 0x65, 0x53, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x73, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x73,
	0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65,
	0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12,
	0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64,
	0x61, 0x74, 0x61, 0x12, 0x1d, 0x0a, 0x0a, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x5f, 0x69,
	0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x49, 0x64, 0x22, 0x7b, 0x0a, 0x13, 0x57, 0x72, 0x69, 0x74, 0x65, 0x53, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x73, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x73, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x49, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x1d,
	0x0a, 0x0a, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x09, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x12, 0x10, 0x0a,
	0x03, 0x65, 0x72, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x65, 0x72, 0x72, 0x22,
	0x72, 0x0a, 0x11, 0x52, 0x65, 0x61, 0x64, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x5f, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49,
	0x64, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x69, 0x7a,
	0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x12, 0x14, 0x0a,
	0x05, 0x77, 0x61, 0x74, 0x63, 0x68, 0x18, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x05, 0x77, 0x61,
	0x74, 0x63, 0x68, 0x22, 0x40, 0x0a, 0x12, 0x52, 0x65, 0x61, 0x64, 0x53, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66,
	0x73, 0x65, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65,
	0x74, 0x12, 0x12, 0x0a, 0x04, 0x44, 0x61, 0x74, 0x61, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x04, 0x44, 0x61, 0x74, 0x61, 0x22, 0x32, 0x0a, 0x14, 0x47, 0x65, 0x74, 0x53, 0x74, 0x72, 0x65,
	0x61, 0x6d, 0x53, 0x74, 0x61, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1a, 0x0a,
	0x08, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x08, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x44, 0x22, 0x5b, 0x0a, 0x15, 0x47, 0x65, 0x74,
	0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x53, 0x74, 0x61, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x44, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x44, 0x12, 0x14,
	0x0a, 0x05, 0x62, 0x65, 0x67, 0x69, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x62,
	0x65, 0x67, 0x69, 0x6e, 0x12, 0x10, 0x0a, 0x03, 0x65, 0x6e, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x03, 0x65, 0x6e, 0x64, 0x32, 0xf1, 0x01, 0x0a, 0x0e, 0x73, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x4a, 0x0a, 0x0b, 0x57, 0x72, 0x69,
	0x74, 0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x12, 0x19, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x1a, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x57, 0x72, 0x69, 0x74,
	0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x28, 0x01, 0x30, 0x01, 0x12, 0x45, 0x0a, 0x0a, 0x52, 0x65, 0x61, 0x64, 0x53, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x12, 0x18, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x52, 0x65, 0x61, 0x64,
	0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x52, 0x65, 0x61, 0x64, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x30, 0x01, 0x12, 0x4c, 0x0a, 0x0d,
	0x47, 0x65, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x53, 0x74, 0x61, 0x74, 0x12, 0x1b, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x47, 0x65, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x53,
	0x74, 0x61, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1c, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x47, 0x65, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x53, 0x74, 0x61, 0x74,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x26, 0x5a, 0x24, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x6b, 0x7a, 0x6a, 0x2f, 0x73, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x49, 0x4f, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x3b, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_streamIO_proto_stream_proto_rawDescOnce sync.Once
	file_streamIO_proto_stream_proto_rawDescData = file_streamIO_proto_stream_proto_rawDesc
)

func file_streamIO_proto_stream_proto_rawDescGZIP() []byte {
	file_streamIO_proto_stream_proto_rawDescOnce.Do(func() {
		file_streamIO_proto_stream_proto_rawDescData = protoimpl.X.CompressGZIP(file_streamIO_proto_stream_proto_rawDescData)
	})
	return file_streamIO_proto_stream_proto_rawDescData
}

var file_streamIO_proto_stream_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_streamIO_proto_stream_proto_goTypes = []interface{}{
	(*WriteStreamRequest)(nil),    // 0: proto.WriteStreamRequest
	(*WriteStreamResponse)(nil),   // 1: proto.WriteStreamResponse
	(*ReadStreamRequest)(nil),     // 2: proto.ReadStreamRequest
	(*ReadStreamResponse)(nil),    // 3: proto.ReadStreamResponse
	(*GetStreamStatRequest)(nil),  // 4: proto.GetStreamStatRequest
	(*GetStreamStatResponse)(nil), // 5: proto.GetStreamStatResponse
}
var file_streamIO_proto_stream_proto_depIdxs = []int32{
	0, // 0: proto.stream_service.WriteStream:input_type -> proto.WriteStreamRequest
	2, // 1: proto.stream_service.ReadStream:input_type -> proto.ReadStreamRequest
	4, // 2: proto.stream_service.GetStreamStat:input_type -> proto.GetStreamStatRequest
	1, // 3: proto.stream_service.WriteStream:output_type -> proto.WriteStreamResponse
	3, // 4: proto.stream_service.ReadStream:output_type -> proto.ReadStreamResponse
	5, // 5: proto.stream_service.GetStreamStat:output_type -> proto.GetStreamStatResponse
	3, // [3:6] is the sub-list for method output_type
	0, // [0:3] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_streamIO_proto_stream_proto_init() }
func file_streamIO_proto_stream_proto_init() {
	if File_streamIO_proto_stream_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_streamIO_proto_stream_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteStreamRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_streamIO_proto_stream_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteStreamResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_streamIO_proto_stream_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadStreamRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_streamIO_proto_stream_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadStreamResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_streamIO_proto_stream_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetStreamStatRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_streamIO_proto_stream_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetStreamStatResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_streamIO_proto_stream_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_streamIO_proto_stream_proto_goTypes,
		DependencyIndexes: file_streamIO_proto_stream_proto_depIdxs,
		MessageInfos:      file_streamIO_proto_stream_proto_msgTypes,
	}.Build()
	File_streamIO_proto_stream_proto = out.File
	file_streamIO_proto_stream_proto_rawDesc = nil
	file_streamIO_proto_stream_proto_goTypes = nil
	file_streamIO_proto_stream_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// StreamServiceClient is the client API for StreamService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type StreamServiceClient interface {
	WriteStream(ctx context.Context, opts ...grpc.CallOption) (StreamService_WriteStreamClient, error)
	ReadStream(ctx context.Context, in *ReadStreamRequest, opts ...grpc.CallOption) (StreamService_ReadStreamClient, error)
	GetStreamStat(ctx context.Context, in *GetStreamStatRequest, opts ...grpc.CallOption) (*GetStreamStatResponse, error)
}

type streamServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewStreamServiceClient(cc grpc.ClientConnInterface) StreamServiceClient {
	return &streamServiceClient{cc}
}

func (c *streamServiceClient) WriteStream(ctx context.Context, opts ...grpc.CallOption) (StreamService_WriteStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &_StreamService_serviceDesc.Streams[0], "/proto.stream_service/WriteStream", opts...)
	if err != nil {
		return nil, err
	}
	x := &streamServiceWriteStreamClient{stream}
	return x, nil
}

type StreamService_WriteStreamClient interface {
	Send(*WriteStreamRequest) error
	Recv() (*WriteStreamResponse, error)
	grpc.ClientStream
}

type streamServiceWriteStreamClient struct {
	grpc.ClientStream
}

func (x *streamServiceWriteStreamClient) Send(m *WriteStreamRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *streamServiceWriteStreamClient) Recv() (*WriteStreamResponse, error) {
	m := new(WriteStreamResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *streamServiceClient) ReadStream(ctx context.Context, in *ReadStreamRequest, opts ...grpc.CallOption) (StreamService_ReadStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &_StreamService_serviceDesc.Streams[1], "/proto.stream_service/ReadStream", opts...)
	if err != nil {
		return nil, err
	}
	x := &streamServiceReadStreamClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type StreamService_ReadStreamClient interface {
	Recv() (*ReadStreamResponse, error)
	grpc.ClientStream
}

type streamServiceReadStreamClient struct {
	grpc.ClientStream
}

func (x *streamServiceReadStreamClient) Recv() (*ReadStreamResponse, error) {
	m := new(ReadStreamResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *streamServiceClient) GetStreamStat(ctx context.Context, in *GetStreamStatRequest, opts ...grpc.CallOption) (*GetStreamStatResponse, error) {
	out := new(GetStreamStatResponse)
	err := c.cc.Invoke(ctx, "/proto.stream_service/GetStreamStat", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// StreamServiceServer is the server API for StreamService service.
type StreamServiceServer interface {
	WriteStream(StreamService_WriteStreamServer) error
	ReadStream(*ReadStreamRequest, StreamService_ReadStreamServer) error
	GetStreamStat(context.Context, *GetStreamStatRequest) (*GetStreamStatResponse, error)
}

// UnimplementedStreamServiceServer can be embedded to have forward compatible implementations.
type UnimplementedStreamServiceServer struct {
}

func (*UnimplementedStreamServiceServer) WriteStream(StreamService_WriteStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method WriteStream not implemented")
}
func (*UnimplementedStreamServiceServer) ReadStream(*ReadStreamRequest, StreamService_ReadStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method ReadStream not implemented")
}
func (*UnimplementedStreamServiceServer) GetStreamStat(context.Context, *GetStreamStatRequest) (*GetStreamStatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetStreamStat not implemented")
}

func RegisterStreamServiceServer(s *grpc.Server, srv StreamServiceServer) {
	s.RegisterService(&_StreamService_serviceDesc, srv)
}

func _StreamService_WriteStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(StreamServiceServer).WriteStream(&streamServiceWriteStreamServer{stream})
}

type StreamService_WriteStreamServer interface {
	Send(*WriteStreamResponse) error
	Recv() (*WriteStreamRequest, error)
	grpc.ServerStream
}

type streamServiceWriteStreamServer struct {
	grpc.ServerStream
}

func (x *streamServiceWriteStreamServer) Send(m *WriteStreamResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *streamServiceWriteStreamServer) Recv() (*WriteStreamRequest, error) {
	m := new(WriteStreamRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _StreamService_ReadStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ReadStreamRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StreamServiceServer).ReadStream(m, &streamServiceReadStreamServer{stream})
}

type StreamService_ReadStreamServer interface {
	Send(*ReadStreamResponse) error
	grpc.ServerStream
}

type streamServiceReadStreamServer struct {
	grpc.ServerStream
}

func (x *streamServiceReadStreamServer) Send(m *ReadStreamResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _StreamService_GetStreamStat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetStreamStatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StreamServiceServer).GetStreamStat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.stream_service/GetStreamStat",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StreamServiceServer).GetStreamStat(ctx, req.(*GetStreamStatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _StreamService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "proto.stream_service",
	HandlerType: (*StreamServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetStreamStat",
			Handler:    _StreamService_GetStreamStat_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "WriteStream",
			Handler:       _StreamService_WriteStream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "ReadStream",
			Handler:       _StreamService_ReadStream_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "streamIO/proto/stream.proto",
}
