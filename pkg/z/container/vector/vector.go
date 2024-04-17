package vector

import (
	"errors"
	"mo_join/pkg/vm/mheap"
	"mo_join/pkg/z/container/nulls"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/encoding"
	"reflect"
)

func New(typ types.Type) *Vector {
	switch typ.Oid {
	case types.T_int8:
		return &Vector{
			Typ: typ,
			Col: []int8{},
		}
	}
	return nil
}

func Clean(v *Vector, m *mheap.Mheap) {
	if !v.Or && v.Data != nil {
		mheap.Free(m, v.Data)
		v.Data = nil
	}
}
func (v *Vector) IsScalar() bool {
	return v.IsConst
}

func Length(v *Vector) int {
	if v.IsScalar() {
		return v.Length
	}
	switch v.Typ.Oid {
	default:
		return reflect.ValueOf(v.Col).Len()
	}
}

func UnionBatch(v, w *Vector, offset int64, cnt int, flags []uint8, m *mheap.Mheap) error {
	if v.Or {
		return errors.New("UnionOne operation cannot be performed for origin vector")
	}

	oldLen := Length(v)

	switch v.Typ.Oid {

	case types.T_int8:
		col := w.Col.([]int8)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize))
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeInt8Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]int8)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt8Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}
	}

	for i, j := 0, uint64(oldLen); i < len(flags); i++ {
		if flags[i] > 0 {
			if nulls.Contains(w.Nsp, uint64(offset)+uint64(i)) {
				nulls.Add(v.Nsp, j)
			}
			j++
		}
	}
	return nil
}

func UnionOne(v, w *Vector, sel int64, m *mheap.Mheap) error {
	if v.Or {
		return errors.New("UnionOne operation cannot be performed for origin vector")
	}
	switch v.Typ.Oid {

	case types.T_int8:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeInt8Slice(data)
			vs[0] = w.Col.([]int8)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]int8)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+1))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt8Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]int8)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*1]
		}
	}
	if nulls.Any(w.Nsp) && nulls.Contains(w.Nsp, uint64(sel)) {
		nulls.Add(v.Nsp, uint64(Length(v)-1))
	}
	return nil
}

func DecodeFixedCol[T any](v *Vector, sz int) []T {
	return encoding.DecodeFixedSlice[T](v.Data, sz)
}
