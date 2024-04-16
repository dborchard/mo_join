package vm

import (
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
)

type OperatorInfo struct {
	Idx           int
	ParallelIdx   int
	ParallelMajor bool
	IsFirst       bool
	IsLast        bool

	CnAddr      string
	OperatorID  int32
	ParallelID  int32
	MaxParallel int32
}

type ExecStatus int

const (
	ExecStop ExecStatus = iota
	ExecNext
	ExecHasMore
)

type CallResult struct {
	Status ExecStatus
	Batch  *batch.Batch
}

func NewCallResult() CallResult {
	return CallResult{
		Status: ExecNext,
	}
}
func CancelCheck(proc *process.Process) (error, bool) {
	select {
	case <-proc.Ctx.Done():
		return proc.Ctx.Err(), true
	default:
		return nil, false
	}
}

var CancelResult = CallResult{
	Status: ExecStop,
}
