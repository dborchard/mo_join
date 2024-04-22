package batch

import (
	"errors"
	"mo_join/pkg/vm/mheap"
	"mo_join/pkg/z/container/ring"
	"mo_join/pkg/z/container/vector"
	"sync/atomic"
)

type Batch struct {
	// Ro if true, Attrs is read only
	Ro bool
	// reference count, default is 1
	Cnt int64
	// SelsData encoded row number list
	SelsData []byte
	// Sels row number list
	Sels []int64
	// Attrs column name list
	Attrs []string
	// Vecs col data
	Vecs []*vector.Vector
	// ring
	Zs   []int64
	As   []string    // alias list
	Refs []uint64    // reference count
	Ht   interface{} // hash table
	Rs   []ring.Ring
}

func NewWithSize(n int) *Batch {
	return &Batch{
		Cnt:  1,
		Vecs: make([]*vector.Vector, n),
	}
}

func (bat *Batch) Clean(m *mheap.Mheap) {
	if atomic.AddInt64(&bat.Cnt, -1) != 0 {
		return
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vector.Clean(vec, m)
		}
	}

	bat.Vecs = nil
	bat.Zs = nil
}

// InitZsOne init Batch.Zs and values are all 1
func (bat *Batch) InitZsOne(len int) {
	bat.Zs = make([]int64, len)
	for i := range bat.Zs {
		bat.Zs[i]++
	}
}

func (bat *Batch) Append(mh *mheap.Mheap, b *Batch) (*Batch, error) {
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
		if err := vector.UnionBatch(bat.Vecs[i], b.Vecs[i], 0, vector.Length(b.Vecs[i]), flags[:vector.Length(b.Vecs[i])], mh); err != nil {
			return nil, err
		}
	}
	bat.Zs = append(bat.Zs, b.Zs...)
	return bat, nil
}

func Clean(bat *Batch, m *mheap.Mheap) {
	if bat.SelsData != nil {
		mheap.Free(m, bat.SelsData)
		bat.Sels = nil
		bat.SelsData = nil
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vector.Clean(vec, m)
		}
	}
	bat.Vecs = nil

	bat.As = nil
	bat.Zs = nil
}
func SetLength(bat *Batch, n int) {
	for _, vec := range bat.Vecs {
		vector.SetLength(vec, n)
	}
	for _, r := range bat.Rs {
		r.SetLength(n)
	}
	bat.Zs = bat.Zs[:n]
}
