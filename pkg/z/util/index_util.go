package util

import (
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/nulls"
	"mo_join/pkg/z/container/vector"
)

type PackerList struct {
	//ps []*types.Packer
}

func (list *PackerList) Free() {

}

func CompactSingleIndexCol(vector *vector.Vector, proc *process.Process) (*vector.Vector, *nulls.Nulls, error) {
	panic("")
}

func SerialWithoutCompacted(vs []*vector.Vector, proc *process.Process, u *PackerList) (*vector.Vector, *nulls.Nulls, error) {
	panic("")
}

func CompactPrimaryCol(v *vector.Vector, bitMap *nulls.Nulls, proc *process.Process) (*vector.Vector, error) {
	panic("")
}
