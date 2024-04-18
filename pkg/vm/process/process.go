package process

import (
	"context"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/mheap"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/vector"
)

type Process struct {
	Reg    Register
	Mp     *mempool.Mempool
	Cancel context.CancelFunc
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

// NewFromProc create a new Process based on another process.
func NewFromProc(m *mheap.Mheap, p *Process, regNumber int) *Process {
	proc := &Process{Mp: m}
	ctx, cancel := context.WithCancel(context.Background())
	// reg and cancel
	proc.Cancel = cancel
	proc.Reg.MergeReceivers = make([]*WaitRegister, regNumber)
	for i := 0; i < regNumber; i++ {
		proc.Reg.MergeReceivers[i] = &WaitRegister{
			Ctx: ctx,
			Ch:  make(chan *batch.Batch, 1),
		}
	}
	return proc
}
