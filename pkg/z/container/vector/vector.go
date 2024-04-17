package vector

import (
	"mojoins/pkg/common/mpool"
	"mojoins/pkg/z/container/types"
)

// GetUnionAllFunction : A more sensible function for copying vector,
// which avoids having to do type conversions and type judgements every time you append.
func GetUnionAllFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector) error {
	return nil
}

func NewConstNull(typ types.Type, length int, mp *mpool.MPool) *Vector {
	vec := &Vector{
		typ: typ,
		//class:  CONSTANT,
		//length: length,
	}

	return vec
}
