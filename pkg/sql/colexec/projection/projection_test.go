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

package projection

import (
	"bytes"
	"mo_join/pkg/sql/plan"
	"mo_join/pkg/vm/mheap"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/container/vector"
	"mo_join/pkg/z/encoding"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type projectionTestCase struct {
	arg   *Argument
	types []types.Type
	proc  *process.Process
}

var (
	tcs []projectionTestCase
)

func init() {
	tcs = []projectionTestCase{
		{
			proc: process.New(mheap.New()),
			types: []types.Type{
				{Oid: types.T_int8},
			},
			arg: &Argument{
				Es: []*plan.Expr{
					{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
				},
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		Prepare(tc.proc, tc.arg)
	}
}

func TestProjection(t *testing.T) {
	for _, tc := range tcs {
		Prepare(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, Rows)
		Call(tc.proc, tc.arg)
		if tc.proc.Reg.InputBatch != nil {
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
		}
		tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, Rows)
		Call(tc.proc, tc.arg)
		if tc.proc.Reg.InputBatch != nil {
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
		}
		tc.proc.Reg.InputBatch = &batch.Batch{}
		Call(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = nil
		Call(tc.proc, tc.arg)
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp))
	}
}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.InitZsOne(int(rows))
	for i := range bat.Vecs {
		vec := vector.New(ts[i])
		switch vec.Typ.Oid {
		case types.T_int8:
			data, err := mheap.Alloc(proc.Mp, rows*1)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt8Slice(vec.Data)[:rows]
			for i := range vs {
				vs[i] = int8(i)
			}
			vec.Col = vs
		}
		bat.Vecs[i] = vec
	}
	return bat
}
