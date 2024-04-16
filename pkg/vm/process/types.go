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
	Ax interface{}
	Ts []interface{}
	Ws []*WaitRegister
}

type Process struct {
	Reg   Register
	Mp    *mempool.Mempool
	Refer map[string]uint64
}
