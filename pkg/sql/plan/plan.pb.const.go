package plan

type Node_JoinFlag int32
type Node_NodeType int32
type Type_TypeId int32

const (
	Node_INNER  Node_JoinFlag = 0
	Node_OUTER  Node_JoinFlag = 1
	Node_SEMI   Node_JoinFlag = 2
	Node_ANTI   Node_JoinFlag = 4
	Node_SINGLE Node_JoinFlag = 8
	Node_MARK   Node_JoinFlag = 16
	Node_APPLY  Node_JoinFlag = 32
)

const (
	Type_INT8 Type_TypeId = 20
)
