package hash

import (
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/container/vector"
	"reflect"
	"unsafe"
)

func Rehash(count int, hs []uint64, vec *vector.Vector) {
	switch vec.Typ.Oid {

	case types.TFloat64:
		vs := vec.Col.([]float64)
		for i := 0; i < count; i++ {
			hs[i] = uint64(F64hash(noescape(unsafe.Pointer(&vs[i])), uintptr(hs[i])))
		}

	case types.TVarchar:
		vs := vec.Col.(*types.Bytes)
		hp := *(*reflect.SliceHeader)(unsafe.Pointer(&vs.Data))
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash(noescape(unsafe.Pointer(hp.Data+uintptr(vs.Offsets[i]))), uintptr(hs[i]), uintptr(vs.Lengths[i])))
		}
	}
}

func RehashSels(sels []int64, hs []uint64, vec *vector.Vector) {
	switch vec.Typ.Oid {

	case types.TFloat64:
		vs := vec.Col.([]float64)
		for _, sel := range sels {
			hs[sel] = uint64(F64hash(noescape(unsafe.Pointer(&vs[sel])), uintptr(hs[sel])))
		}

	case types.TVarchar:
		vs := vec.Col.(*types.Bytes)
		hp := *(*reflect.SliceHeader)(unsafe.Pointer(&vs.Data))
		for _, sel := range sels {
			hs[sel] = uint64(Memhash(noescape(unsafe.Pointer(hp.Data+uintptr(vs.Offsets[sel]))), uintptr(hs[sel]), uintptr(vs.Lengths[sel])))
		}
	}
}
