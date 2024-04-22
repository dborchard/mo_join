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

package group

import (
	"bytes"
	"mo_join/pkg/sql/colexec/aggregate"
	"mo_join/pkg/sql/plan"
	"mo_join/pkg/vm/mheap"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/nulls"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/container/vector"
	"mo_join/pkg/z/encoding"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type groupTestCase struct {
	arg   *Argument
	flgs  []bool // flgs[i] == true: nullable
	types []types.Type
	proc  *process.Process
}

var (
	tcs []groupTestCase
)

func init() {
	tcs = []groupTestCase{
		newTestCase(mheap.New(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []*plan.Expr{}, []aggregate.Aggregate{{Op: 0, E: newExpression(0)}}),
		newTestCase(mheap.New(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []*plan.Expr{newExpression(0)}, []aggregate.Aggregate{{Op: 0, E: newExpression(0)}}),
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

func TestGroup(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.InputBatch = newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		_, err = Call(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.InputBatch = newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		_, err = Call(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.InputBatch = &batch.Batch{}
		_, err = Call(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.InputBatch = nil
		_, err = Call(tc.proc, tc.arg)
		require.NoError(t, err)
		if tc.proc.Reg.InputBatch != nil {
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
		}
		tc.proc.Reg.InputBatch = nil
		_, err = Call(tc.proc, tc.arg)
		require.NoError(t, err)
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp))
	}
}

func BenchmarkGroup(b *testing.B) {
	for i := 0; i < b.N; i++ {

		tcs = []groupTestCase{
			newTestCase(mheap.New(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []*plan.Expr{}, []aggregate.Aggregate{{Op: 0, E: newExpression(0)}}),
			newTestCase(mheap.New(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []*plan.Expr{newExpression(0)}, []aggregate.Aggregate{{Op: 0, E: newExpression(0)}}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.InputBatch = newBatch(t, tc.flgs, tc.types, tc.proc, BenchmarkRows)
			_, err = Call(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.InputBatch = newBatch(t, tc.flgs, tc.types, tc.proc, BenchmarkRows)
			_, err = Call(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.InputBatch = &batch.Batch{}
			_, err = Call(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.InputBatch = nil
			_, err = Call(tc.proc, tc.arg)
			require.NoError(t, err)
			if tc.proc.Reg.InputBatch != nil {
				tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
			}
		}
	}
}

func newTestCase(m *mheap.Mheap, flgs []bool, ts []types.Type, exprs []*plan.Expr, aggs []aggregate.Aggregate) groupTestCase {
	return groupTestCase{
		types: ts,
		flgs:  flgs,
		proc:  process.New(m),
		arg: &Argument{
			Aggs:  aggs,
			Exprs: exprs,
		},
	}
}

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
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
			if flgs[i] {
				nulls.Add(vec.Nsp, uint64(0))
			}
			vec.Col = vs
		}
		bat.Vecs[i] = vec
	}
	return bat
}
