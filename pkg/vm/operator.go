package vm

import (
	"bytes"
	"mojoins/pkg/vm/process"
)

type Operator interface {
	// Free release all the memory allocated from mPool in an operator.
	// pipelineFailed marks the process status of the pipeline when the method is called.
	Free(proc *process.Process, pipelineFailed bool, err error)

	// String returns the string representation of an operator.
	String(buf *bytes.Buffer)

	//Prepare prepares an operator for execution.
	Prepare(proc *process.Process) error

	//Call calls an operator.
	Call(proc *process.Process) (CallResult, error)

	//Release an operator
	Release()

	// OperatorBase methods
	SetInfo(info *OperatorInfo)
	AppendChild(child Operator)

	GetOperatorBase() *OperatorBase
}

type OperatorBase struct {
	OperatorInfo
	Children []Operator
}

func (o *OperatorBase) SetInfo(info *OperatorInfo) {
	o.OperatorInfo = *info
}

func (o *OperatorBase) AppendChild(child Operator) {
	o.Children = append(o.Children, child)
}

func (o *OperatorBase) GetIsLast() bool {
	return o.IsLast
}

func (o *OperatorBase) GetParallelIdx() int {
	return o.ParallelIdx
}

func (o *OperatorBase) GetParallelMajor() bool {
	return o.ParallelMajor
}

func (o *OperatorBase) GetChildren(idx int) Operator {
	return o.Children[idx]
}

func (o *OperatorBase) GetIdx() int {
	return o.Idx
}

func (o *OperatorBase) GetIsFirst() bool {
	return o.IsFirst
}
