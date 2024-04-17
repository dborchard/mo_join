package batch

import (
	"mo_join/pkg/common/hashmap"
	"mo_join/pkg/common/mpool"
	"mo_join/pkg/z/container/vector"
)

type Batch struct {
	Ro    bool
	Attrs []string
	Vecs  []*vector.Vector
	// row count of batch, to instead of old len(Zs).
	rowCount int
	Cnt      int64

	AuxData any // hash table, runtime filter, etc.
}

func (bat *Batch) Clean(m *mpool.MPool) {
}

func (bat *Batch) IsEmpty() bool {
	return bat.rowCount == 0 && bat.AuxData == nil
}

func (bat *Batch) SetVector(pos int32, vec *vector.Vector) {
	bat.Vecs[pos] = vec
}

func (bat *Batch) AddRowCount(rowCount int) {
	bat.rowCount += rowCount
}

func (bat *Batch) RowCount() int {
	return bat.rowCount
}

func (bat *Batch) Last() bool {
	return false
}

func (bat *Batch) SetRowCount(length int) {
	return
}

func (bat *Batch) DupJmAuxData() (ret *hashmap.JoinMap) {
	if bat.AuxData == nil {
		return
	}
	jm := bat.AuxData.(*hashmap.JoinMap)

	ret = jm
	bat.AuxData = nil

	return
}

func (bat *Batch) VectorCount() int {
	return 9
}

func NewWithSize(n int) *Batch {
	return &Batch{
		Cnt:      1,
		Vecs:     make([]*vector.Vector, n),
		rowCount: 0,
	}
}
