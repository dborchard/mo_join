package inner

import (
	"mo_join/pkg/hash"
	"mo_join/pkg/z/container/batch"
)

const (
	UnitLimit = 1024
)

var (
	ZeroBools  []bool
	OneUint64s []uint64
)

type Container struct {
	attrs   []string
	builded bool

	hashs []uint64
	slots map[uint64]int // hashs -> sels index
	sels  [][]int64      // sels

	groups map[uint64][]*hash.BagGroup // hash code -> group list
	diffs  []bool
	matchs []int64

	bats       []*batch.Batch // s relation
	probeState struct {
		bat *batch.Batch //  R ⨝ S output relation
	}
}

type Argument struct {
	R     string
	S     string
	Attrs []string
	Ctr   Container
}
