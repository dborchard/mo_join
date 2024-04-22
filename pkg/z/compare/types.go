package compare

import (
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/vector"
)

type Compare interface {
	Vector() *vector.Vector
	Set(int, *vector.Vector)
	Compare(int, int, int64, int64) int
	Copy(int, int, int64, int64, *process.Process) error
}
