package process

import "mo_join/pkg/vm/mempool"

func New(mp *mempool.Mempool) *Process {
	return &Process{
		Mp: mp,
	}
}

func (p *Process) Size() int64 {
	return p.Mp.Size()
}
