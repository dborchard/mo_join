package batch

import (
	"mo_join/pkg/z/container/vector"
)

type Batch struct {
	Ro       bool
	SelsData []byte
	Sels     []int64
	Attrs    []string
	Vecs     []*vector.Vector
}
