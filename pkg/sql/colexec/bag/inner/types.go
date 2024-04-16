package inner

import (
	"mo_join/pkg/hash"
	"mo_join/pkg/intmap/fastmap"
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
	builded    bool
	diffs      []bool
	matchs     []int64
	hashs      []uint64
	attrs      []string
	sels       [][]int64      // sels
	slots      *fastmap.Map   // hash code -> sels index
	bats       []*batch.Batch // s relation
	probeState struct {
		bat *batch.Batch // output relation
	}
	groups map[uint64][]*hash.BagGroup // hash code -> group list
}

type Argument struct {
	R     string
	S     string
	Attrs []string
	Ctr   Container
}
