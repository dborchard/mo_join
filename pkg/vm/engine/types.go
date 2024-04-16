package engine

import (
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
)

type Segment interface {
	ID() string
	Read([]uint64, []string, *process.Process) (*batch.Batch, error) // read only arguments
}
