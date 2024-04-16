package vm

const (
	BagInnerJoin = iota
	Transfer
	Output
)

type Instruction struct {
	Op  int
	Arg interface{}
}

type Instructions []Instruction
