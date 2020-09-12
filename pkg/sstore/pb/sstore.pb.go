// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.12.3
// source: streamIO/pkg/sstore/pb/sstore.proto

package pb

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

type Version struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term  int64 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	Index int64 `protobuf:"varint,2,opt,name=index,proto3" json:"index,omitempty"`
}

func (x *Version) Reset() {
	*x = Version{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Version) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Version) ProtoMessage() {}

func (x *Version) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Version.ProtoReflect.Descriptor instead.
func (*Version) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{0}
}

func (x *Version) GetTerm() int64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *Version) GetIndex() int64 {
	if x != nil {
		return x.Index
	}
	return 0
}

type Entry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamID int64    `protobuf:"varint,2,opt,name=streamID,proto3" json:"streamID,omitempty"`
	Offset   int64    `protobuf:"varint,3,opt,name=offset,proto3" json:"offset,omitempty"`
	Data     []byte   `protobuf:"bytes,5,opt,name=data,proto3" json:"data,omitempty"`
	Ver      *Version `protobuf:"bytes,4,opt,name=ver,proto3" json:"ver,omitempty"`
}

func (x *Entry) Reset() {
	*x = Entry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Entry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entry) ProtoMessage() {}

func (x *Entry) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Entry.ProtoReflect.Descriptor instead.
func (*Entry) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{1}
}

func (x *Entry) GetStreamID() int64 {
	if x != nil {
		return x.StreamID
	}
	return 0
}

func (x *Entry) GetOffset() int64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *Entry) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *Entry) GetVer() *Version {
	if x != nil {
		return x.Ver
	}
	return nil
}

type OffsetInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamID int64  `protobuf:"varint,1,opt,name=streamID,proto3" json:"streamID,omitempty"`
	Begin    int64  `protobuf:"varint,2,opt,name=begin,proto3" json:"begin,omitempty"`
	Offset   int64  `protobuf:"varint,3,opt,name=offset,proto3" json:"offset,omitempty"`
	End      int64  `protobuf:"varint,4,opt,name=end,proto3" json:"end,omitempty"`
	CRC      uint32 `protobuf:"varint,5,opt,name=CRC,proto3" json:"CRC,omitempty"`
}

func (x *OffsetInfo) Reset() {
	*x = OffsetInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OffsetInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OffsetInfo) ProtoMessage() {}

func (x *OffsetInfo) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OffsetInfo.ProtoReflect.Descriptor instead.
func (*OffsetInfo) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{2}
}

func (x *OffsetInfo) GetStreamID() int64 {
	if x != nil {
		return x.StreamID
	}
	return 0
}

func (x *OffsetInfo) GetBegin() int64 {
	if x != nil {
		return x.Begin
	}
	return 0
}

func (x *OffsetInfo) GetOffset() int64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *OffsetInfo) GetEnd() int64 {
	if x != nil {
		return x.End
	}
	return 0
}

func (x *OffsetInfo) GetCRC() uint32 {
	if x != nil {
		return x.CRC
	}
	return 0
}

type SegmentMeta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	From        *Version              `protobuf:"bytes,1,opt,name=From,proto3" json:"From,omitempty"`
	To          *Version              `protobuf:"bytes,2,opt,name=To,proto3" json:"To,omitempty"`
	CreateTS    int64                 `protobuf:"varint,3,opt,name=CreateTS,proto3" json:"CreateTS,omitempty"`
	OffSetInfos map[int64]*OffsetInfo `protobuf:"bytes,4,rep,name=OffSetInfos,proto3" json:"OffSetInfos,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *SegmentMeta) Reset() {
	*x = SegmentMeta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SegmentMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SegmentMeta) ProtoMessage() {}

func (x *SegmentMeta) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SegmentMeta.ProtoReflect.Descriptor instead.
func (*SegmentMeta) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{3}
}

func (x *SegmentMeta) GetFrom() *Version {
	if x != nil {
		return x.From
	}
	return nil
}

func (x *SegmentMeta) GetTo() *Version {
	if x != nil {
		return x.To
	}
	return nil
}

func (x *SegmentMeta) GetCreateTS() int64 {
	if x != nil {
		return x.CreateTS
	}
	return 0
}

func (x *SegmentMeta) GetOffSetInfos() map[int64]*OffsetInfo {
	if x != nil {
		return x.OffSetInfos
	}
	return nil
}

type JournalMeta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Old      bool     `protobuf:"varint,1,opt,name=Old,proto3" json:"Old,omitempty"`
	Filename string   `protobuf:"bytes,2,opt,name=Filename,proto3" json:"Filename,omitempty"`
	Version  string   `protobuf:"bytes,3,opt,name=Version,proto3" json:"Version,omitempty"`
	From     *Version `protobuf:"bytes,4,opt,name=From,proto3" json:"From,omitempty"`
	To       *Version `protobuf:"bytes,5,opt,name=To,proto3" json:"To,omitempty"`
}

func (x *JournalMeta) Reset() {
	*x = JournalMeta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JournalMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JournalMeta) ProtoMessage() {}

func (x *JournalMeta) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JournalMeta.ProtoReflect.Descriptor instead.
func (*JournalMeta) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{4}
}

func (x *JournalMeta) GetOld() bool {
	if x != nil {
		return x.Old
	}
	return false
}

func (x *JournalMeta) GetFilename() string {
	if x != nil {
		return x.Filename
	}
	return ""
}

func (x *JournalMeta) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *JournalMeta) GetFrom() *Version {
	if x != nil {
		return x.From
	}
	return nil
}

func (x *JournalMeta) GetTo() *Version {
	if x != nil {
		return x.To
	}
	return nil
}

type AppendJournal struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Filename string `protobuf:"bytes,1,opt,name=Filename,proto3" json:"Filename,omitempty"`
}

func (x *AppendJournal) Reset() {
	*x = AppendJournal{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendJournal) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendJournal) ProtoMessage() {}

func (x *AppendJournal) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendJournal.ProtoReflect.Descriptor instead.
func (*AppendJournal) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{5}
}

func (x *AppendJournal) GetFilename() string {
	if x != nil {
		return x.Filename
	}
	return ""
}

type DeleteJournal struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Filename string `protobuf:"bytes,1,opt,name=Filename,proto3" json:"Filename,omitempty"`
}

func (x *DeleteJournal) Reset() {
	*x = DeleteJournal{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteJournal) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteJournal) ProtoMessage() {}

func (x *DeleteJournal) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteJournal.ProtoReflect.Descriptor instead.
func (*DeleteJournal) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{6}
}

func (x *DeleteJournal) GetFilename() string {
	if x != nil {
		return x.Filename
	}
	return ""
}

type AppendSegment struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Filename string `protobuf:"bytes,1,opt,name=Filename,proto3" json:"Filename,omitempty"`
}

func (x *AppendSegment) Reset() {
	*x = AppendSegment{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendSegment) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendSegment) ProtoMessage() {}

func (x *AppendSegment) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendSegment.ProtoReflect.Descriptor instead.
func (*AppendSegment) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{7}
}

func (x *AppendSegment) GetFilename() string {
	if x != nil {
		return x.Filename
	}
	return ""
}

type DeleteSegment struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Filename string `protobuf:"bytes,1,opt,name=Filename,proto3" json:"Filename,omitempty"`
}

func (x *DeleteSegment) Reset() {
	*x = DeleteSegment{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteSegment) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteSegment) ProtoMessage() {}

func (x *DeleteSegment) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteSegment.ProtoReflect.Descriptor instead.
func (*DeleteSegment) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{8}
}

func (x *DeleteSegment) GetFilename() string {
	if x != nil {
		return x.Filename
	}
	return ""
}

type DelJournalHeader struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Filename string `protobuf:"bytes,1,opt,name=Filename,proto3" json:"Filename,omitempty"`
}

func (x *DelJournalHeader) Reset() {
	*x = DelJournalHeader{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DelJournalHeader) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DelJournalHeader) ProtoMessage() {}

func (x *DelJournalHeader) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DelJournalHeader.ProtoReflect.Descriptor instead.
func (*DelJournalHeader) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{9}
}

func (x *DelJournalHeader) GetFilename() string {
	if x != nil {
		return x.Filename
	}
	return ""
}

type FileIndex struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SegmentIndex  int64 `protobuf:"varint,5,opt,name=segment_index,json=segmentIndex,proto3" json:"segment_index,omitempty"`
	JournalIndex  int64 `protobuf:"varint,6,opt,name=journal_index,json=journalIndex,proto3" json:"journal_index,omitempty"`
	ManifestIndex int64 `protobuf:"varint,7,opt,name=manifest_index,json=manifestIndex,proto3" json:"manifest_index,omitempty"`
}

func (x *FileIndex) Reset() {
	*x = FileIndex{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FileIndex) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FileIndex) ProtoMessage() {}

func (x *FileIndex) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FileIndex.ProtoReflect.Descriptor instead.
func (*FileIndex) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{10}
}

func (x *FileIndex) GetSegmentIndex() int64 {
	if x != nil {
		return x.SegmentIndex
	}
	return 0
}

func (x *FileIndex) GetJournalIndex() int64 {
	if x != nil {
		return x.JournalIndex
	}
	return 0
}

func (x *FileIndex) GetManifestIndex() int64 {
	if x != nil {
		return x.ManifestIndex
	}
	return 0
}

type ManifestSnapshot struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FileIndex      *FileIndex              `protobuf:"bytes,5,opt,name=file_index,json=fileIndex,proto3" json:"file_index,omitempty"`
	Version        *Version                `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
	Segments       []string                `protobuf:"bytes,2,rep,name=segments,proto3" json:"segments,omitempty"`
	Journals       []string                `protobuf:"bytes,3,rep,name=journals,proto3" json:"journals,omitempty"`
	JournalHeaders map[string]*JournalMeta `protobuf:"bytes,4,rep,name=journal_headers,json=journalHeaders,proto3" json:"journal_headers,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *ManifestSnapshot) Reset() {
	*x = ManifestSnapshot{}
	if protoimpl.UnsafeEnabled {
		mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[11]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ManifestSnapshot) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ManifestSnapshot) ProtoMessage() {}

func (x *ManifestSnapshot) ProtoReflect() protoreflect.Message {
	mi := &file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[11]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ManifestSnapshot.ProtoReflect.Descriptor instead.
func (*ManifestSnapshot) Descriptor() ([]byte, []int) {
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP(), []int{11}
}

func (x *ManifestSnapshot) GetFileIndex() *FileIndex {
	if x != nil {
		return x.FileIndex
	}
	return nil
}

func (x *ManifestSnapshot) GetVersion() *Version {
	if x != nil {
		return x.Version
	}
	return nil
}

func (x *ManifestSnapshot) GetSegments() []string {
	if x != nil {
		return x.Segments
	}
	return nil
}

func (x *ManifestSnapshot) GetJournals() []string {
	if x != nil {
		return x.Journals
	}
	return nil
}

func (x *ManifestSnapshot) GetJournalHeaders() map[string]*JournalMeta {
	if x != nil {
		return x.JournalHeaders
	}
	return nil
}

var File_streamIO_pkg_sstore_pb_sstore_proto protoreflect.FileDescriptor

var file_streamIO_pkg_sstore_pb_sstore_proto_rawDesc = []byte{
	0x0a, 0x23, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x4f, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x73,
	0x73, 0x74, 0x6f, 0x72, 0x65, 0x2f, 0x70, 0x62, 0x2f, 0x73, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x02, 0x70, 0x62, 0x22, 0x33, 0x0a, 0x07, 0x56, 0x65, 0x72,
	0x73, 0x69, 0x6f, 0x6e, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x14, 0x0a, 0x05, 0x69, 0x6e, 0x64, 0x65,
	0x78, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x22, 0x6e,
	0x0a, 0x05, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x1a, 0x0a, 0x08, 0x73, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x49, 0x44, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x73, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x49, 0x44, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x64,
	0x61, 0x74, 0x61, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x12,
	0x1d, 0x0a, 0x03, 0x76, 0x65, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x70,
	0x62, 0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x03, 0x76, 0x65, 0x72, 0x22, 0x7a,
	0x0a, 0x0a, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x1a, 0x0a, 0x08,
	0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08,
	0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x44, 0x12, 0x14, 0x0a, 0x05, 0x62, 0x65, 0x67, 0x69,
	0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x62, 0x65, 0x67, 0x69, 0x6e, 0x12, 0x16,
	0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06,
	0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x65, 0x6e, 0x64, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x03, 0x65, 0x6e, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x43, 0x52, 0x43, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x03, 0x43, 0x52, 0x43, 0x22, 0xfb, 0x01, 0x0a, 0x0b, 0x53,
	0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x1f, 0x0a, 0x04, 0x46, 0x72,
	0x6f, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x70, 0x62, 0x2e, 0x56, 0x65,
	0x72, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x04, 0x46, 0x72, 0x6f, 0x6d, 0x12, 0x1b, 0x0a, 0x02, 0x54,
	0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x70, 0x62, 0x2e, 0x56, 0x65, 0x72,
	0x73, 0x69, 0x6f, 0x6e, 0x52, 0x02, 0x54, 0x6f, 0x12, 0x1a, 0x0a, 0x08, 0x43, 0x72, 0x65, 0x61,
	0x74, 0x65, 0x54, 0x53, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x43, 0x72, 0x65, 0x61,
	0x74, 0x65, 0x54, 0x53, 0x12, 0x42, 0x0a, 0x0b, 0x4f, 0x66, 0x66, 0x53, 0x65, 0x74, 0x49, 0x6e,
	0x66, 0x6f, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x70, 0x62, 0x2e, 0x53,
	0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x2e, 0x4f, 0x66, 0x66, 0x53, 0x65,
	0x74, 0x49, 0x6e, 0x66, 0x6f, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x0b, 0x4f, 0x66, 0x66,
	0x53, 0x65, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x73, 0x1a, 0x4e, 0x0a, 0x10, 0x4f, 0x66, 0x66, 0x53,
	0x65, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03,
	0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x24,
	0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e,
	0x70, 0x62, 0x2e, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x93, 0x01, 0x0a, 0x0b, 0x4a, 0x6f, 0x75,
	0x72, 0x6e, 0x61, 0x6c, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x10, 0x0a, 0x03, 0x4f, 0x6c, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x03, 0x4f, 0x6c, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x46, 0x69,
	0x6c, 0x65, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x46, 0x69,
	0x6c, 0x65, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x12, 0x1f, 0x0a, 0x04, 0x46, 0x72, 0x6f, 0x6d, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b,
	0x2e, 0x70, 0x62, 0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x04, 0x46, 0x72, 0x6f,
	0x6d, 0x12, 0x1b, 0x0a, 0x02, 0x54, 0x6f, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e,
	0x70, 0x62, 0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x02, 0x54, 0x6f, 0x22, 0x2b,
	0x0a, 0x0d, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x4a, 0x6f, 0x75, 0x72, 0x6e, 0x61, 0x6c, 0x12,
	0x1a, 0x0a, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0x2b, 0x0a, 0x0d, 0x44,
	0x65, 0x6c, 0x65, 0x74, 0x65, 0x4a, 0x6f, 0x75, 0x72, 0x6e, 0x61, 0x6c, 0x12, 0x1a, 0x0a, 0x08,
	0x46, 0x69, 0x6c, 0x65, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08,
	0x46, 0x69, 0x6c, 0x65, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0x2b, 0x0a, 0x0d, 0x41, 0x70, 0x70, 0x65,
	0x6e, 0x64, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x46, 0x69, 0x6c,
	0x65, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x46, 0x69, 0x6c,
	0x65, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0x2b, 0x0a, 0x0d, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53,
	0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x6e, 0x61,
	0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x6e, 0x61,
	0x6d, 0x65, 0x22, 0x2e, 0x0a, 0x10, 0x44, 0x65, 0x6c, 0x4a, 0x6f, 0x75, 0x72, 0x6e, 0x61, 0x6c,
	0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x1a, 0x0a, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x6e, 0x61,
	0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x6e, 0x61,
	0x6d, 0x65, 0x22, 0x7c, 0x0a, 0x09, 0x46, 0x69, 0x6c, 0x65, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x12,
	0x23, 0x0a, 0x0d, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x6e, 0x64, 0x65, 0x78,
	0x18, 0x05, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0c, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x49,
	0x6e, 0x64, 0x65, 0x78, 0x12, 0x23, 0x0a, 0x0d, 0x6a, 0x6f, 0x75, 0x72, 0x6e, 0x61, 0x6c, 0x5f,
	0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x06, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0c, 0x6a, 0x6f, 0x75,
	0x72, 0x6e, 0x61, 0x6c, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x25, 0x0a, 0x0e, 0x6d, 0x61, 0x6e,
	0x69, 0x66, 0x65, 0x73, 0x74, 0x5f, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x07, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x0d, 0x6d, 0x61, 0x6e, 0x69, 0x66, 0x65, 0x73, 0x74, 0x49, 0x6e, 0x64, 0x65, 0x78,
	0x22, 0xc6, 0x02, 0x0a, 0x10, 0x4d, 0x61, 0x6e, 0x69, 0x66, 0x65, 0x73, 0x74, 0x53, 0x6e, 0x61,
	0x70, 0x73, 0x68, 0x6f, 0x74, 0x12, 0x2c, 0x0a, 0x0a, 0x66, 0x69, 0x6c, 0x65, 0x5f, 0x69, 0x6e,
	0x64, 0x65, 0x78, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x70, 0x62, 0x2e, 0x46,
	0x69, 0x6c, 0x65, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x52, 0x09, 0x66, 0x69, 0x6c, 0x65, 0x49, 0x6e,
	0x64, 0x65, 0x78, 0x12, 0x25, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x70, 0x62, 0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1a, 0x0a, 0x08, 0x73, 0x65,
	0x67, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x08, 0x73, 0x65,
	0x67, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x6a, 0x6f, 0x75, 0x72, 0x6e, 0x61,
	0x6c, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x08, 0x6a, 0x6f, 0x75, 0x72, 0x6e, 0x61,
	0x6c, 0x73, 0x12, 0x51, 0x0a, 0x0f, 0x6a, 0x6f, 0x75, 0x72, 0x6e, 0x61, 0x6c, 0x5f, 0x68, 0x65,
	0x61, 0x64, 0x65, 0x72, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x28, 0x2e, 0x70, 0x62,
	0x2e, 0x4d, 0x61, 0x6e, 0x69, 0x66, 0x65, 0x73, 0x74, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f,
	0x74, 0x2e, 0x4a, 0x6f, 0x75, 0x72, 0x6e, 0x61, 0x6c, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x73,
	0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x0e, 0x6a, 0x6f, 0x75, 0x72, 0x6e, 0x61, 0x6c, 0x48, 0x65,
	0x61, 0x64, 0x65, 0x72, 0x73, 0x1a, 0x52, 0x0a, 0x13, 0x4a, 0x6f, 0x75, 0x72, 0x6e, 0x61, 0x6c,
	0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03,
	0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x25,
	0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e,
	0x70, 0x62, 0x2e, 0x4a, 0x6f, 0x75, 0x72, 0x6e, 0x61, 0x6c, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x05,
	0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x42, 0x2b, 0x5a, 0x29, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x6b, 0x7a, 0x6a, 0x2f, 0x73, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x49, 0x4f, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x73, 0x73, 0x74, 0x6f, 0x72, 0x65,
	0x2f, 0x70, 0x62, 0x3b, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_streamIO_pkg_sstore_pb_sstore_proto_rawDescOnce sync.Once
	file_streamIO_pkg_sstore_pb_sstore_proto_rawDescData = file_streamIO_pkg_sstore_pb_sstore_proto_rawDesc
)

func file_streamIO_pkg_sstore_pb_sstore_proto_rawDescGZIP() []byte {
	file_streamIO_pkg_sstore_pb_sstore_proto_rawDescOnce.Do(func() {
		file_streamIO_pkg_sstore_pb_sstore_proto_rawDescData = protoimpl.X.CompressGZIP(file_streamIO_pkg_sstore_pb_sstore_proto_rawDescData)
	})
	return file_streamIO_pkg_sstore_pb_sstore_proto_rawDescData
}

var file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes = make([]protoimpl.MessageInfo, 14)
var file_streamIO_pkg_sstore_pb_sstore_proto_goTypes = []interface{}{
	(*Version)(nil),          // 0: pb.Version
	(*Entry)(nil),            // 1: pb.Entry
	(*OffsetInfo)(nil),       // 2: pb.OffsetInfo
	(*SegmentMeta)(nil),      // 3: pb.SegmentMeta
	(*JournalMeta)(nil),      // 4: pb.JournalMeta
	(*AppendJournal)(nil),    // 5: pb.AppendJournal
	(*DeleteJournal)(nil),    // 6: pb.DeleteJournal
	(*AppendSegment)(nil),    // 7: pb.AppendSegment
	(*DeleteSegment)(nil),    // 8: pb.DeleteSegment
	(*DelJournalHeader)(nil), // 9: pb.DelJournalHeader
	(*FileIndex)(nil),        // 10: pb.FileIndex
	(*ManifestSnapshot)(nil), // 11: pb.ManifestSnapshot
	nil,                      // 12: pb.SegmentMeta.OffSetInfosEntry
	nil,                      // 13: pb.ManifestSnapshot.JournalHeadersEntry
}
var file_streamIO_pkg_sstore_pb_sstore_proto_depIdxs = []int32{
	0,  // 0: pb.Entry.ver:type_name -> pb.Version
	0,  // 1: pb.SegmentMeta.From:type_name -> pb.Version
	0,  // 2: pb.SegmentMeta.To:type_name -> pb.Version
	12, // 3: pb.SegmentMeta.OffSetInfos:type_name -> pb.SegmentMeta.OffSetInfosEntry
	0,  // 4: pb.JournalMeta.From:type_name -> pb.Version
	0,  // 5: pb.JournalMeta.To:type_name -> pb.Version
	10, // 6: pb.ManifestSnapshot.file_index:type_name -> pb.FileIndex
	0,  // 7: pb.ManifestSnapshot.version:type_name -> pb.Version
	13, // 8: pb.ManifestSnapshot.journal_headers:type_name -> pb.ManifestSnapshot.JournalHeadersEntry
	2,  // 9: pb.SegmentMeta.OffSetInfosEntry.value:type_name -> pb.OffsetInfo
	4,  // 10: pb.ManifestSnapshot.JournalHeadersEntry.value:type_name -> pb.JournalMeta
	11, // [11:11] is the sub-list for method output_type
	11, // [11:11] is the sub-list for method input_type
	11, // [11:11] is the sub-list for extension type_name
	11, // [11:11] is the sub-list for extension extendee
	0,  // [0:11] is the sub-list for field type_name
}

func init() { file_streamIO_pkg_sstore_pb_sstore_proto_init() }
func file_streamIO_pkg_sstore_pb_sstore_proto_init() {
	if File_streamIO_pkg_sstore_pb_sstore_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Version); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Entry); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OffsetInfo); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SegmentMeta); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JournalMeta); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AppendJournal); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteJournal); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AppendSegment); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteSegment); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DelJournalHeader); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[10].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FileIndex); i {
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
		file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes[11].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ManifestSnapshot); i {
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
			RawDescriptor: file_streamIO_pkg_sstore_pb_sstore_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   14,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_streamIO_pkg_sstore_pb_sstore_proto_goTypes,
		DependencyIndexes: file_streamIO_pkg_sstore_pb_sstore_proto_depIdxs,
		MessageInfos:      file_streamIO_pkg_sstore_pb_sstore_proto_msgTypes,
	}.Build()
	File_streamIO_pkg_sstore_pb_sstore_proto = out.File
	file_streamIO_pkg_sstore_pb_sstore_proto_rawDesc = nil
	file_streamIO_pkg_sstore_pb_sstore_proto_goTypes = nil
	file_streamIO_pkg_sstore_pb_sstore_proto_depIdxs = nil
}