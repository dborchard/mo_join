package mheap

import "mo_join/pkg/vm/mempool"

type Mheap = mempool.Mempool

func New() *Mheap {
	return mempool.New(1<<32, 8)
}

func Size(m *Mheap) int64 {
	return m.Size()
}

func Alloc(m *Mheap, size int64) ([]byte, error) {
	data := mempool.Alloc(m, int(size))
	return data[:size], nil
}

func Free(m *Mheap, data []byte) {
	//m.Gm.Free(int64(cap(data)))
}

func Grow(m *Mheap, old []byte, size int64) ([]byte, error) {
	data, err := Alloc(m, size)
	if err != nil {
		return nil, err
	}
	copy(data, old)
	return data[:size], nil
}
