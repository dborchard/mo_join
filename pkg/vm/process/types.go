package process

import (
	"context"
	"mo_join/pkg/common/mpool"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/container/vector"
	"time"
)

// Process contains context used in query execution
// one or more pipeline will be generated for one query,
// and one pipeline has one process instance.
type Process struct {
	Ctx context.Context
	Reg Register
}

func (proc *Process) Mp() *mpool.MPool {
	return nil
}

func (proc *Process) GetMPool() *mpool.MPool {
	return nil
}

func (proc *Process) PutBatch(bat *batch.Batch) {

}

func (proc *Process) GetVector(typ types.Type) *vector.Vector {
	return vector.NewVec(typ)
}

func (proc *Process) GetAnalyze(idx, parallelIdx int, parallelMajor bool) Analyze {
	return nil
	//if idx >= len(proc.AnalInfos) || idx < 0 {
	//	return &analyze{analInfo: nil, parallelIdx: parallelIdx, parallelMajor: parallelMajor}
	//}
	//return &analyze{analInfo: proc.AnalInfos[idx], wait: 0, parallelIdx: parallelIdx, parallelMajor: parallelMajor}
}

// Register used in execution pipeline and shared with all operators of the same pipeline.
type Register struct {
	// Ss, temporarily stores the row number list in the execution of operators,
	// and it can be reused in the future execution.
	Ss [][]int64
	// InputBatch, stores the result of the previous operator.
	InputBatch *batch.Batch
	// MergeReceivers, receives result of multi previous operators from other pipelines
	// e.g. merge operator.
	MergeReceivers []*WaitRegister
}

// WaitRegister channel
type WaitRegister struct {
	Ctx context.Context
	Ch  chan *batch.Batch
}

// Analyze analyzes information for operator
type Analyze interface {
	Stop()
	ChildrenCallStop(time.Time)
	Start()
	Alloc(int64)
	Input(*batch.Batch, bool)
	Output(*batch.Batch, bool)
	WaitStop(time.Time)
	DiskIO(*batch.Batch)
	S3IOByte(*batch.Batch)
	S3IOInputCount(int)
	S3IOOutputCount(int)
	Network(*batch.Batch)
	AddScanTime(t time.Time)
	AddInsertTime(t time.Time)
}
