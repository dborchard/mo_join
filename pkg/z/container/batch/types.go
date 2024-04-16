package batch

import (
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/vector"
)

type Batch struct {
	Ro       bool
	SelsData []byte
	Sels     []int64
	Attrs    []string
	Vecs     []*vector.Vector
}

func (bat *Batch) Clean(proc *process.Process) {
	if bat.SelsData != nil {
		proc.Free(bat.SelsData)
		bat.Sels = nil
		bat.SelsData = nil
	}
	for _, vec := range bat.Vecs {
		vec.Clean(proc)
	}
}
