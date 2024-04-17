package process

import (
	"context"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/z/container/batch"
)

type WaitRegister struct {
	Ctx context.Context
	Ch  chan *batch.Batch
}

type Register struct {
	InputBatch     *batch.Batch
	MergeReceivers []*WaitRegister
}

type Process struct {
	Reg Register
	Mp  *mempool.Mempool
}

func (p *Process) Alloc(size int64) ([]byte, error) {
	data := p.Mp.Alloc(int(size))
	return data, nil
}

func (p *Process) Free(data []byte) bool {
	end := p.Mp.Free(data)
	return end
}
