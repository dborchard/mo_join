package plan

type isExpr_Expr interface {
	isExpr_Expr()
	MarshalTo([]byte) (int, error)
	ProtoSize() int
}

type Type_TypeId int32

const (
	Type_INT8 Type_TypeId = 20
)

type Type struct {
	Id                   Type_TypeId `protobuf:"varint,1,opt,name=id,proto3,enum=plan.Type_TypeId" json:"id,omitempty"`
	Nullable             bool        `protobuf:"varint,2,opt,name=nullable,proto3" json:"nullable,omitempty"`
	Width                int32       `protobuf:"varint,3,opt,name=width,proto3" json:"width,omitempty"`
	Precision            int32       `protobuf:"varint,4,opt,name=precision,proto3" json:"precision,omitempty"`
	Size                 int32       `protobuf:"varint,5,opt,name=size,proto3" json:"size,omitempty"`
	Scale                int32       `protobuf:"varint,6,opt,name=scale,proto3" json:"scale,omitempty"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}
type Expr struct {
	Typ       *Type  `protobuf:"bytes,1,opt,name=typ,proto3" json:"typ,omitempty"`
	TableName string `protobuf:"bytes,2,opt,name=table_name,json=tableName,proto3" json:"table_name,omitempty"`
	ColName   string `protobuf:"bytes,3,opt,name=col_name,json=colName,proto3" json:"col_name,omitempty"`
	// Types that are valid to be assigned to Expr:
	//	*Expr_C
	//	*Expr_P
	//	*Expr_V
	//	*Expr_Col
	//	*Expr_F
	//	*Expr_List
	//	*Expr_Sub
	//	*Expr_Corr
	//	*Expr_T
	Expr                 isExpr_Expr `protobuf_oneof:"expr"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

type Expr_Col struct {
	Col *ColRef `protobuf:"bytes,7,opt,name=col,proto3,oneof"`
}

// Reference a column in the proj list of a node.
type ColRef struct {
	RelPos               int32    `protobuf:"varint,1,opt,name=rel_pos,json=relPos,proto3" json:"rel_pos,omitempty"`
	ColPos               int32    `protobuf:"varint,2,opt,name=col_pos,json=colPos,proto3" json:"col_pos,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}
