// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package join

import (
	"mo_join/pkg/sql/plan"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/hashtable"
	"mo_join/pkg/z/container/vector"
)

const (
	Build = iota
	Probe
	End
)

const (
	UnitLimit = 256
)

var OneInt64s []int64

type evalVector struct {
	needFree bool
	vec      *vector.Vector
}

type Container struct {
	// indicates if addition columns need to be copied
	flg bool

	// projection
	vecs   []evalVector
	colPos []int32 // pos of vectors need to be copied

	state  int
	rows   uint64
	hashes []uint64
	sels   [][]int64
	bat    *batch.Batch

	// build phase
	keys          [][]byte
	strHashMap    *hashtable.StringHashMap
	strHashStates [][3]uint64
	zValues       []int64
	inserted      []uint8
	zInserted     []uint8
	values        []uint64
}

type Argument struct {
	ctr        *Container
	IsPreBuild bool // hashtable is pre-built

	// received from USER
	Result     []ResultPos   // onList, arg1 = arg2
	Conditions [][]Condition // projections Condition[0] - R, Condition[1] - S.
}

type ResultPos struct {
	Rel int32
	Pos int32
}

type Condition struct {
	Scale int32
	Expr  *plan.Expr
}
