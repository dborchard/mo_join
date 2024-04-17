package plan

import "github.com/gogo/protobuf/proto"

type Query struct {
	StmtType Query_StatementType `protobuf:"varint,1,opt,name=stmt_type,json=stmtType,proto3,enum=plan.Query_StatementType" json:"stmt_type,omitempty"`
	Steps    []int32             `protobuf:"varint,2,rep,packed,name=steps,proto3" json:"steps,omitempty"`
	Nodes    []*Node             `protobuf:"bytes,3,rep,name=nodes,proto3" json:"nodes,omitempty"`
	Params   []*Expr             `protobuf:"bytes,4,rep,name=params,proto3" json:"params,omitempty"`
	Headings []string            `protobuf:"bytes,5,rep,name=headings,proto3" json:"headings,omitempty"`
	LoadTag  bool                `protobuf:"varint,6,opt,name=loadTag,proto3" json:"loadTag,omitempty"`
}

type Query_StatementType int32

const (
	Query_UNKNOWN Query_StatementType = 0
	Query_SELECT  Query_StatementType = 1
	Query_INSERT  Query_StatementType = 2
	Query_REPLACE Query_StatementType = 3
	Query_DELETE  Query_StatementType = 4
	Query_UPDATE  Query_StatementType = 5
	Query_MERGE   Query_StatementType = 6
)

var Query_StatementType_name = map[int32]string{
	0: "UNKNOWN",
	1: "SELECT",
	2: "INSERT",
	3: "REPLACE",
	4: "DELETE",
	5: "UPDATE",
	6: "MERGE",
}

var Query_StatementType_value = map[string]int32{
	"UNKNOWN": 0,
	"SELECT":  1,
	"INSERT":  2,
	"REPLACE": 3,
	"DELETE":  4,
	"UPDATE":  5,
	"MERGE":   6,
}

func (x Query_StatementType) String() string {
	return proto.EnumName(Query_StatementType_name, int32(x))
}
