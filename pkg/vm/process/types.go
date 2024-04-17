package process

import (
	"mo_join/pkg/vm/mempool"
	"sync"
)

type WaitRegister struct {
	Wg *sync.WaitGroup
	Ch chan interface{}
}

type Register struct {
	NextBatch     interface{}
	WaitRegisters []*WaitRegister
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
