package process

import (
	"context"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/z/container/batch"
)

type Process struct {
	Reg Register
	Mp  *mempool.Mempool
}
type Register struct {
	InputBatch     *batch.Batch
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
