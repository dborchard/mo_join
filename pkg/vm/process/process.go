package process

import (
	"context"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/vector"
)

type Process struct {
	Reg Register
	Mp  *mempool.Mempool
}
type Register struct {
	InputBatch     *batch.Batch
	Vecs           []*vector.Vector
	MergeReceivers []*WaitRegister
}

type WaitRegister struct {
	Ctx context.Context
	Ch  chan *batch.Batch
}

func New(mp *mempool.Mempool) *Process {
	return &Process{
		Mp: mp,
	}
}

func FreeRegisters(proc *Process) {
	for _, vec := range proc.Reg.Vecs {
		vec.Ref = 0
		vector.Free(vec, proc.Mp)
	}
	proc.Reg.Vecs = proc.Reg.Vecs[:0]
}
