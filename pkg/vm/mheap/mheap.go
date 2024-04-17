package mheap

import "mo_join/pkg/vm/mempool"

type Mheap = mempool.Mempool

func Free(m *Mheap, data []byte) {
	//m.Gm.Free(int64(cap(data)))
}

func Alloc(m *Mheap, size int64) ([]byte, error) {
	data := mempool.Alloc(m, int(size))
	return data[:size], nil
}
