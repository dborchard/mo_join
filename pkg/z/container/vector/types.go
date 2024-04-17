package vector

import (
	"mo_join/pkg/z/container/nulls"
	"mo_join/pkg/z/container/types"
)

type Vector struct {
	Or   bool   // true: origin
	Ref  uint64 // reference count
	Link uint64 // link count
	Data []byte // raw data
	Typ  types.Type
	Col  interface{}  // column data, encoded Data
	Nsp  *nulls.Nulls // nulls list

	// some attributes for const vector (a vector with a lot of rows of a same const value)
	IsConst bool
	Length  int
}
