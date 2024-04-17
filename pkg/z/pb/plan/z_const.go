package plan

import (
	proto "github.com/gogo/protobuf/proto"
)

type Node_NodeType int32

const (
	Node_UNKNOWN Node_NodeType = 0
	// Scans
	Node_VALUE_SCAN    Node_NodeType = 1
	Node_TABLE_SCAN    Node_NodeType = 2
	Node_FUNCTION_SCAN Node_NodeType = 3
	Node_EXTERNAL_SCAN Node_NodeType = 4
	Node_MATERIAL_SCAN Node_NodeType = 5
	Node_SOURCE_SCAN   Node_NodeType = 6
	// Proj, for convenience
	Node_PROJECT Node_NodeType = 10
	// External function call (UDF)
	Node_EXTERNAL_FUNCTION Node_NodeType = 11
	// Material, CTE, etc.
	Node_MATERIAL       Node_NodeType = 20
	Node_RECURSIVE_CTE  Node_NodeType = 21
	Node_SINK           Node_NodeType = 22
	Node_SINK_SCAN      Node_NodeType = 23
	Node_RECURSIVE_SCAN Node_NodeType = 24
	// Proper Relational Operators
	Node_AGG       Node_NodeType = 30
	Node_DISTINCT  Node_NodeType = 31
	Node_FILTER    Node_NodeType = 32
	Node_JOIN      Node_NodeType = 33
	Node_SAMPLE    Node_NodeType = 34
	Node_SORT      Node_NodeType = 35
	Node_UNION     Node_NodeType = 36
	Node_UNION_ALL Node_NodeType = 37
	Node_UNIQUE    Node_NodeType = 38
	Node_WINDOW    Node_NodeType = 39
	// Physical tuple mover
	Node_BROADCAST Node_NodeType = 40
	Node_SPLIT     Node_NodeType = 41
	Node_GATHER    Node_NodeType = 42
	// Misc
	Node_ASSERT Node_NodeType = 50
	//
	Node_INSERT  Node_NodeType = 51
	Node_DELETE  Node_NodeType = 52
	Node_REPLACE Node_NodeType = 53
	//
	Node_LOCK_OP Node_NodeType = 54
	//
	Node_INTERSECT     Node_NodeType = 55
	Node_INTERSECT_ALL Node_NodeType = 56
	Node_MINUS         Node_NodeType = 57
	Node_MINUS_ALL     Node_NodeType = 58
	//
	Node_ON_DUPLICATE_KEY Node_NodeType = 59
	Node_PRE_INSERT       Node_NodeType = 60
	Node_PRE_DELETE       Node_NodeType = 61
	// the node which build insert batch for hidden table(unique key)
	Node_PRE_INSERT_UK Node_NodeType = 62
	Node_PRE_INSERT_SK Node_NodeType = 63
	//
	Node_TIME_WINDOW  Node_NodeType = 64
	Node_FILL         Node_NodeType = 65
	Node_PARTITION    Node_NodeType = 66
	Node_FUZZY_FILTER Node_NodeType = 67
)

var Node_NodeType_name = map[int32]string{
	0:  "UNKNOWN",
	1:  "VALUE_SCAN",
	2:  "TABLE_SCAN",
	3:  "FUNCTION_SCAN",
	4:  "EXTERNAL_SCAN",
	5:  "MATERIAL_SCAN",
	6:  "SOURCE_SCAN",
	10: "PROJECT",
	11: "EXTERNAL_FUNCTION",
	20: "MATERIAL",
	21: "RECURSIVE_CTE",
	22: "SINK",
	23: "SINK_SCAN",
	24: "RECURSIVE_SCAN",
	30: "AGG",
	31: "DISTINCT",
	32: "FILTER",
	33: "JOIN",
	34: "SAMPLE",
	35: "SORT",
	36: "UNION",
	37: "UNION_ALL",
	38: "UNIQUE",
	39: "WINDOW",
	40: "BROADCAST",
	41: "SPLIT",
	42: "GATHER",
	50: "ASSERT",
	51: "INSERT",
	52: "DELETE",
	53: "REPLACE",
	54: "LOCK_OP",
	55: "INTERSECT",
	56: "INTERSECT_ALL",
	57: "MINUS",
	58: "MINUS_ALL",
	59: "ON_DUPLICATE_KEY",
	60: "PRE_INSERT",
	61: "PRE_DELETE",
	62: "PRE_INSERT_UK",
	63: "PRE_INSERT_SK",
	64: "TIME_WINDOW",
	65: "FILL",
	66: "PARTITION",
	67: "FUZZY_FILTER",
}

var Node_NodeType_value = map[string]int32{
	"UNKNOWN":           0,
	"VALUE_SCAN":        1,
	"TABLE_SCAN":        2,
	"FUNCTION_SCAN":     3,
	"EXTERNAL_SCAN":     4,
	"MATERIAL_SCAN":     5,
	"SOURCE_SCAN":       6,
	"PROJECT":           10,
	"EXTERNAL_FUNCTION": 11,
	"MATERIAL":          20,
	"RECURSIVE_CTE":     21,
	"SINK":              22,
	"SINK_SCAN":         23,
	"RECURSIVE_SCAN":    24,
	"AGG":               30,
	"DISTINCT":          31,
	"FILTER":            32,
	"JOIN":              33,
	"SAMPLE":            34,
	"SORT":              35,
	"UNION":             36,
	"UNION_ALL":         37,
	"UNIQUE":            38,
	"WINDOW":            39,
	"BROADCAST":         40,
	"SPLIT":             41,
	"GATHER":            42,
	"ASSERT":            50,
	"INSERT":            51,
	"DELETE":            52,
	"REPLACE":           53,
	"LOCK_OP":           54,
	"INTERSECT":         55,
	"INTERSECT_ALL":     56,
	"MINUS":             57,
	"MINUS_ALL":         58,
	"ON_DUPLICATE_KEY":  59,
	"PRE_INSERT":        60,
	"PRE_DELETE":        61,
	"PRE_INSERT_UK":     62,
	"PRE_INSERT_SK":     63,
	"TIME_WINDOW":       64,
	"FILL":              65,
	"PARTITION":         66,
	"FUZZY_FILTER":      67,
}

func (x Node_NodeType) String() string {
	return proto.EnumName(Node_NodeType_name, int32(x))
}

type Node_JoinType int32

const (
	Node_INNER  Node_JoinType = 0
	Node_LEFT   Node_JoinType = 1
	Node_RIGHT  Node_JoinType = 2
	Node_OUTER  Node_JoinType = 3
	Node_SEMI   Node_JoinType = 4
	Node_ANTI   Node_JoinType = 5
	Node_SINGLE Node_JoinType = 6
	Node_MARK   Node_JoinType = 7
	Node_APPLY  Node_JoinType = 8
	Node_INDEX  Node_JoinType = 9
)

var Node_JoinType_name = map[int32]string{
	0: "INNER",
	1: "LEFT",
	2: "RIGHT",
	3: "OUTER",
	4: "SEMI",
	5: "ANTI",
	6: "SINGLE",
	7: "MARK",
	8: "APPLY",
	9: "INDEX",
}

var Node_JoinType_value = map[string]int32{
	"INNER":  0,
	"LEFT":   1,
	"RIGHT":  2,
	"OUTER":  3,
	"SEMI":   4,
	"ANTI":   5,
	"SINGLE": 6,
	"MARK":   7,
	"APPLY":  8,
	"INDEX":  9,
}

func (x Node_JoinType) String() string {
	return proto.EnumName(Node_JoinType_name, int32(x))
}
