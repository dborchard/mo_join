package vector

import (
	"mo_join/pkg/common/mpool"
	"mo_join/pkg/z/container/types"
)

// Vector represent a column
type Vector struct {
	// type represent the type of column
	typ    types.Type
	sorted bool // for some optimization

}

func NewVec(typ types.Type) *Vector {
	vec := &Vector{
		typ: typ,
	}

	return vec
}

func (v *Vector) UnmarshalBinary(data []byte) error {
	return nil
}

func (v *Vector) Free(mp any) {

}

func (v *Vector) GetType() *types.Type {
	return nil
}

func (v *Vector) Length() int {
	return 0
}

func (v *Vector) UnionBatch(w *Vector, offset int64, cnt int, flags []uint8, mp *mpool.MPool) error {
	panic("")
}

func (v *Vector) SetSorted(b bool) {
	v.sorted = b
}

func (v *Vector) GetSorted() bool {
	return v.sorted
}

func (v *Vector) IsConstNull() bool {
	return false
}

func (v *Vector) UnionOne(w *Vector, sel int64, mp *mpool.MPool) error {
	return nil
}

// GetConstSetFunction A more sensible function for const vector set,
// which avoids having to do type conversions and type judgements every time you append.
func GetConstSetFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector, sel int64, length int) error {
	return nil
}
