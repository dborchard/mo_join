package plan

type PreInsertUkCtx struct {
	Columns              []int32   `protobuf:"varint,1,rep,packed,name=columns,proto3" json:"columns,omitempty"`
	PkColumn             int32     `protobuf:"varint,2,opt,name=pk_column,json=pkColumn,proto3" json:"pk_column,omitempty"`
	PkType               *Type     `protobuf:"bytes,3,opt,name=pk_type,json=pkType,proto3" json:"pk_type,omitempty"`
	UkType               *Type     `protobuf:"bytes,4,opt,name=uk_type,json=ukType,proto3" json:"uk_type,omitempty"`
	TableDef             *TableDef `protobuf:"bytes,5,opt,name=table_def,json=tableDef,proto3" json:"table_def,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}
