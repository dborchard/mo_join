package vm

const (
	Join = iota
	Semi
	Left
	Merge
	Product
	Connector
	Complement
)

// Instruction contains relational algebra
type Instruction struct {
	// Op specified the operator code of an instruction.
	Op int
	// Arg contains the operand of this instruction.
	Arg interface{}
}

type Instructions []Instruction
