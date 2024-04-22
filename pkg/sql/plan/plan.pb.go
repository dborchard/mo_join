package plan

import (
	proto "github.com/gogo/protobuf/proto"
)

type isExpr_Expr interface {
	isExpr_Expr()
	MarshalTo([]byte) (int, error)
	ProtoSize() int
}

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

func (m *Expr) Reset() { *m = Expr{} }

func (*Expr) ProtoMessage() {}

func (m *Expr) String() string { return proto.CompactTextString(m) }

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

type Node struct {
	NodeType    Node_NodeType `protobuf:"varint,1,opt,name=node_type,json=nodeType,proto3,enum=plan.Node_NodeType" json:"node_type,omitempty"`
	NodeId      int32         `protobuf:"varint,2,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	ProjectList []*Expr       `protobuf:"bytes,4,rep,name=project_list,json=projectList,proto3" json:"project_list,omitempty"`
	Children    []int32       `protobuf:"varint,5,rep,packed,name=children,proto3" json:"children,omitempty"`
	JoinType    Node_JoinFlag `protobuf:"varint,6,opt,name=join_type,json=joinType,proto3,enum=plan.Node_JoinFlag" json:"join_type,omitempty"`
	OnList      []*Expr       `protobuf:"bytes,7,rep,name=on_list,json=onList,proto3" json:"on_list,omitempty"`
	WhereList   []*Expr       `protobuf:"bytes,8,rep,name=where_list,json=whereList,proto3" json:"where_list,omitempty"`
	GroupBy     []*Expr       `protobuf:"bytes,9,rep,name=group_by,json=groupBy,proto3" json:"group_by,omitempty"`
	GroupingSet []*Expr       `protobuf:"bytes,10,rep,name=grouping_set,json=groupingSet,proto3" json:"grouping_set,omitempty"`
	AggList     []*Expr       `protobuf:"bytes,11,rep,name=agg_list,json=aggList,proto3" json:"agg_list,omitempty"`
	Limit       *Expr         `protobuf:"bytes,15,opt,name=limit,proto3" json:"limit,omitempty"`
	Offset      *Expr         `protobuf:"bytes,16,opt,name=offset,proto3" json:"offset,omitempty"`
	//TableDef    *TableDef     `protobuf:"bytes,17,opt,name=table_def,json=tableDef,proto3" json:"table_def,omitempty"`
	//ObjRef      *ObjectRef    `protobuf:"bytes,18,opt,name=obj_ref,json=objRef,proto3" json:"obj_ref,omitempty"`
	//RowsetData  *RowsetData   `protobuf:"bytes,19,opt,name=rowset_data,json=rowsetData,proto3" json:"rowset_data,omitempty"`
}

type Expr_F struct {
	F *Function `protobuf:"bytes,8,opt,name=f,proto3,oneof"`
}

type Function struct {
	Func                 *ObjectRef `protobuf:"bytes,1,opt,name=func,proto3" json:"func,omitempty"`
	Args                 []*Expr    `protobuf:"bytes,2,rep,name=args,proto3" json:"args,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

// Object ref, reference a object in database, 4 part name.
type ObjectRef struct {
	Server               int64    `protobuf:"varint,1,opt,name=server,proto3" json:"server,omitempty"`
	Db                   int64    `protobuf:"varint,2,opt,name=db,proto3" json:"db,omitempty"`
	Schema               int64    `protobuf:"varint,3,opt,name=schema,proto3" json:"schema,omitempty"`
	Obj                  int64    `protobuf:"varint,4,opt,name=obj,proto3" json:"obj,omitempty"`
	ServerName           string   `protobuf:"bytes,5,opt,name=server_name,json=serverName,proto3" json:"server_name,omitempty"`
	DbName               string   `protobuf:"bytes,6,opt,name=db_name,json=dbName,proto3" json:"db_name,omitempty"`
	SchemaName           string   `protobuf:"bytes,7,opt,name=schema_name,json=schemaName,proto3" json:"schema_name,omitempty"`
	ObjName              string   `protobuf:"bytes,8,opt,name=obj_name,json=objName,proto3" json:"obj_name,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

type Plan struct {
	Plan                 isPlan_Plan `protobuf_oneof:"plan"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}
type isPlan_Plan interface {
	isPlan_Plan()
	MarshalTo([]byte) (int, error)
	ProtoSize() int
}
