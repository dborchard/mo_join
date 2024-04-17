package plan

type Type struct {
	Id                   int32    `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	NotNullable          bool     `protobuf:"varint,2,opt,name=notNullable,proto3" json:"notNullable,omitempty"`
	AutoIncr             bool     `protobuf:"varint,3,opt,name=auto_incr,json=autoIncr,proto3" json:"auto_incr,omitempty"`
	Width                int32    `protobuf:"varint,4,opt,name=width,proto3" json:"width,omitempty"`
	Scale                int32    `protobuf:"varint,5,opt,name=scale,proto3" json:"scale,omitempty"`
	Table                string   `protobuf:"bytes,6,opt,name=table,proto3" json:"table,omitempty"`
	Enumvalues           string   `protobuf:"bytes,7,opt,name=enumvalues,proto3" json:"enumvalues,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}
