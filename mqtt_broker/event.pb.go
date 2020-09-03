// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.12.3
// source: streamIO/mqtt-broker/event.proto

package mqtt_broker

import (
	proto "github.com/golang/protobuf/proto"
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

type SubscribeEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamId       int64            `protobuf:"varint,1,opt,name=stream_id,json=streamId,proto3" json:"stream_id,omitempty"`
	SessionId      int64            `protobuf:"varint,2,opt,name=session_id,json=sessionId,proto3" json:"session_id,omitempty"`
	StreamServerId int64            `protobuf:"varint,3,opt,name=stream_server_id,json=streamServerId,proto3" json:"stream_server_id,omitempty"`
	Topic          map[string]int32 `protobuf:"bytes,4,rep,name=topic,proto3" json:"topic,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
}

func (x *SubscribeEvent) Reset() {
	*x = SubscribeEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_mqtt_broker_event_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SubscribeEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubscribeEvent) ProtoMessage() {}

func (x *SubscribeEvent) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_mqtt_broker_event_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubscribeEvent.ProtoReflect.Descriptor instead.
func (*SubscribeEvent) Descriptor() ([]byte, []int) {
	return file_streamIO_mqtt_broker_event_proto_rawDescGZIP(), []int{0}
}

func (x *SubscribeEvent) GetStreamId() int64 {
	if x != nil {
		return x.StreamId
	}
	return 0
}

func (x *SubscribeEvent) GetSessionId() int64 {
	if x != nil {
		return x.SessionId
	}
	return 0
}

func (x *SubscribeEvent) GetStreamServerId() int64 {
	if x != nil {
		return x.StreamServerId
	}
	return 0
}

func (x *SubscribeEvent) GetTopic() map[string]int32 {
	if x != nil {
		return x.Topic
	}
	return nil
}

type UnSubscribeEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamId       int64            `protobuf:"varint,1,opt,name=stream_id,json=streamId,proto3" json:"stream_id,omitempty"`
	SessionId      int64            `protobuf:"varint,2,opt,name=session_id,json=sessionId,proto3" json:"session_id,omitempty"`
	StreamServerId int64            `protobuf:"varint,3,opt,name=stream_server_id,json=streamServerId,proto3" json:"stream_server_id,omitempty"`
	Topic          map[string]int32 `protobuf:"bytes,4,rep,name=topic,proto3" json:"topic,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
}

func (x *UnSubscribeEvent) Reset() {
	*x = UnSubscribeEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_mqtt_broker_event_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UnSubscribeEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UnSubscribeEvent) ProtoMessage() {}

func (x *UnSubscribeEvent) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_mqtt_broker_event_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UnSubscribeEvent.ProtoReflect.Descriptor instead.
func (*UnSubscribeEvent) Descriptor() ([]byte, []int) {
	return file_streamIO_mqtt_broker_event_proto_rawDescGZIP(), []int{1}
}

func (x *UnSubscribeEvent) GetStreamId() int64 {
	if x != nil {
		return x.StreamId
	}
	return 0
}

func (x *UnSubscribeEvent) GetSessionId() int64 {
	if x != nil {
		return x.SessionId
	}
	return 0
}

func (x *UnSubscribeEvent) GetStreamServerId() int64 {
	if x != nil {
		return x.StreamServerId
	}
	return 0
}

func (x *UnSubscribeEvent) GetTopic() map[string]int32 {
	if x != nil {
		return x.Topic
	}
	return nil
}

type RetainMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *RetainMessage) Reset() {
	*x = RetainMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_mqtt_broker_event_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RetainMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RetainMessage) ProtoMessage() {}

func (x *RetainMessage) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_mqtt_broker_event_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RetainMessage.ProtoReflect.Descriptor instead.
func (*RetainMessage) Descriptor() ([]byte, []int) {
	return file_streamIO_mqtt_broker_event_proto_rawDescGZIP(), []int{2}
}

func (x *RetainMessage) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

var File_streamIO_mqtt_broker_event_proto protoreflect.FileDescriptor

var file_streamIO_mqtt_broker_event_proto_rawDesc = []byte{
	0x0a, 0x20, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x4f, 0x2f, 0x6d, 0x71, 0x74, 0x74, 0x2d,
	0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x2f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x0b, 0x6d, 0x71, 0x74, 0x74, 0x5f, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x22,
	0xee, 0x01, 0x0a, 0x0e, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x45, 0x76, 0x65,
	0x6e, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x5f, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x64, 0x12,
	0x1d, 0x0a, 0x0a, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x09, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x28,
	0x0a, 0x10, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x5f,
	0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0e, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d,
	0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x49, 0x64, 0x12, 0x3c, 0x0a, 0x05, 0x74, 0x6f, 0x70, 0x69,
	0x63, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x26, 0x2e, 0x6d, 0x71, 0x74, 0x74, 0x5f, 0x62,
	0x72, 0x6f, 0x6b, 0x65, 0x72, 0x2e, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x45,
	0x76, 0x65, 0x6e, 0x74, 0x2e, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52,
	0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x1a, 0x38, 0x0a, 0x0a, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01,
	0x22, 0xf2, 0x01, 0x0a, 0x10, 0x55, 0x6e, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65,
	0x45, 0x76, 0x65, 0x6e, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x5f,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d,
	0x49, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49,
	0x64, 0x12, 0x28, 0x0a, 0x10, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x5f, 0x73, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0e, 0x73, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x49, 0x64, 0x12, 0x3e, 0x0a, 0x05, 0x74,
	0x6f, 0x70, 0x69, 0x63, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x28, 0x2e, 0x6d, 0x71, 0x74,
	0x74, 0x5f, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x2e, 0x55, 0x6e, 0x53, 0x75, 0x62, 0x73, 0x63,
	0x72, 0x69, 0x62, 0x65, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x2e, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x52, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x1a, 0x38, 0x0a, 0x0a, 0x54,
	0x6f, 0x70, 0x69, 0x63, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x23, 0x0a, 0x0d, 0x52, 0x65, 0x74, 0x61, 0x69, 0x6e, 0x4d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x42, 0x32, 0x5a, 0x30, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x6b, 0x7a, 0x6a, 0x2f, 0x73, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x49, 0x4f, 0x2f, 0x6d, 0x71, 0x74, 0x74, 0x5f, 0x62, 0x72, 0x6f, 0x6b,
	0x65, 0x72, 0x3b, 0x6d, 0x71, 0x74, 0x74, 0x5f, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_streamIO_mqtt_broker_event_proto_rawDescOnce sync.Once
	file_streamIO_mqtt_broker_event_proto_rawDescData = file_streamIO_mqtt_broker_event_proto_rawDesc
)

func file_streamIO_mqtt_broker_event_proto_rawDescGZIP() []byte {
	file_streamIO_mqtt_broker_event_proto_rawDescOnce.Do(func() {
		file_streamIO_mqtt_broker_event_proto_rawDescData = protoimpl.X.CompressGZIP(file_streamIO_mqtt_broker_event_proto_rawDescData)
	})
	return file_streamIO_mqtt_broker_event_proto_rawDescData
}

var file_streamIO_mqtt_broker_event_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_streamIO_mqtt_broker_event_proto_goTypes = []interface{}{
	(*SubscribeEvent)(nil),   // 0: mqtt_broker.SubscribeEvent
	(*UnSubscribeEvent)(nil), // 1: mqtt_broker.UnSubscribeEvent
	(*RetainMessage)(nil),    // 2: mqtt_broker.RetainMessage
	nil,                      // 3: mqtt_broker.SubscribeEvent.TopicEntry
	nil,                      // 4: mqtt_broker.UnSubscribeEvent.TopicEntry
}
var file_streamIO_mqtt_broker_event_proto_depIdxs = []int32{
	3, // 0: mqtt_broker.SubscribeEvent.topic:type_name -> mqtt_broker.SubscribeEvent.TopicEntry
	4, // 1: mqtt_broker.UnSubscribeEvent.topic:type_name -> mqtt_broker.UnSubscribeEvent.TopicEntry
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_streamIO_mqtt_broker_event_proto_init() }
func file_streamIO_mqtt_broker_event_proto_init() {
	if File_streamIO_mqtt_broker_event_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_streamIO_mqtt_broker_event_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SubscribeEvent); i {
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
		file_streamIO_mqtt_broker_event_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UnSubscribeEvent); i {
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
		file_streamIO_mqtt_broker_event_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RetainMessage); i {
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
			RawDescriptor: file_streamIO_mqtt_broker_event_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_streamIO_mqtt_broker_event_proto_goTypes,
		DependencyIndexes: file_streamIO_mqtt_broker_event_proto_depIdxs,
		MessageInfos:      file_streamIO_mqtt_broker_event_proto_msgTypes,
	}.Build()
	File_streamIO_mqtt_broker_event_proto = out.File
	file_streamIO_mqtt_broker_event_proto_rawDesc = nil
	file_streamIO_mqtt_broker_event_proto_goTypes = nil
	file_streamIO_mqtt_broker_event_proto_depIdxs = nil
}
