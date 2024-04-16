package vector

import (
	"mo_join/pkg/z/container/nulls"
	"mo_join/pkg/z/container/types"
)

type Vector struct {
	Or   bool   // true: origin
	Data []byte // raw data
	Typ  types.Type
	Col  interface{}
	Nsp  *nulls.Nulls
}
