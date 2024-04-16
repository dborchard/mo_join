package vector

import (
	"mo_join/pkg/z/container/nulls"
	"mo_join/pkg/z/container/types"
	"unsafe"
)

type Vector struct {
	Or   bool   // true: origin
	Data []byte // raw data
	Typ  types.Type
	Col  interface{}
	Nsp  *nulls.Nulls
}

// emptyInterface is the header for an interface{} value.
type emptyInterface struct {
	_    *int
	word unsafe.Pointer
}
