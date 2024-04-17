package indexjoin

import (
	"mo_join/pkg/sql/colexec"
	"mo_join/pkg/vm"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/pb/plan"
)

var _ vm.Operator = new(Argument)

const (
	Probe = iota
	End
)

type container struct {
	colexec.ReceiverOperator
	state int
}

type Argument struct {
	ctr                *container
	Result             []int32
	Typs               []types.Type
	buf                *batch.Batch
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	vm.OperatorBase
}

func NewArgument() *Argument {
	return &Argument{}
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func (arg *Argument) Release() {

}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := arg.ctr
	if ctr != nil {
		ctr.FreeAllReg()
	}
	if arg.buf != nil {
		arg.buf.Clean(proc.Mp())
	}
}

func (arg *Argument) GetIdx() int {
	return 0
}
