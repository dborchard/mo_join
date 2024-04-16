package fastmap

import "sync"

var Pool = sync.Pool{
	New: func() interface{} {
		return New()
	},
}

func New() *Map {
	vs := make([][]int, Group)
	ks := make([][]uint64, Group)
	for i := 0; i < Group; i++ {
		vs[i] = make([]int, 0, 16)
		ks[i] = make([]uint64, 0, 16)
	}
	return &Map{Ks: ks, Vs: vs}
}
