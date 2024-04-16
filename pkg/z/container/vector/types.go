package vector

import (
	"mo_join/pkg/vm/process"
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

func (v *Vector) Clean(p *process.Process) {
	if v.Data != nil {
		if p.Free(v.Data) {
			v.Data = nil
		}
	}
}
