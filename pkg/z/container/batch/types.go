package batch

import (
	"mo_join/pkg/z/container/vector"
)

type Batch struct {
	Ro    bool
	Attrs []string
	Vecs  []*vector.Vector
	// row count of batch, to instead of old len(Zs).
	rowCount int
	Cnt      int64

	AuxData any // hash table, runtime filter, etc.
}
