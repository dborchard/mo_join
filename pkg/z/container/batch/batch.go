package batch

import (
	"bytes"
	"errors"
	"mo_join/pkg/vm/mheap"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/vector"
)

type Batch struct {
	Ht   any     // anything
	Zs   []int64 // ring
	Vecs []*vector.Vector
}

func New(n int) *Batch {
	return &Batch{
		Vecs: make([]*vector.Vector, n),
	}
}

func (bat *Batch) Clean(proc *process.Process) {
	for _, vec := range bat.Vecs {
		vec.Free(proc)
	}
}

func (bat *Batch) String() string {
	var buf bytes.Buffer
	//TODO: need to fix this
	return buf.String()
}

func (bat *Batch) Append(mp *mheap.Mheap, b *Batch) (*Batch, error) {
	if bat == nil {
		return b, nil
	}
	if len(bat.Vecs) != len(b.Vecs) {
		return nil, errors.New("unexpected error happens in batch append")
	}
	if len(bat.Vecs) == 0 {
		return bat, nil
	}
	flags := make([]uint8, vector.Length(b.Vecs[0]))
	for i := range flags {
		flags[i]++
	}
	for i := range bat.Vecs {
		if err := vector.UnionBatch(bat.Vecs[i], b.Vecs[i], 0, vector.Length(b.Vecs[i]), flags[:vector.Length(b.Vecs[i])], mp); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func Clean(bat *Batch, m *mheap.Mheap) {
	for _, vec := range bat.Vecs {
		if vec != nil {
			vector.Clean(vec, m)
		}
	}
	bat.Vecs = nil
}
