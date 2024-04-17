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
	"bytes"
	"context"
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
	Rows = 10 // default rows
)

// add unit tests for cases
type joinTestCase struct {
	arg    *Argument
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []joinTestCase
)

func init() {
	tcs = []joinTestCase{
		newTestCase(mheap.New(), []bool{true}, []types.Type{{Oid: types.T_int8}}, []ResultPos{{0, 0}},
			[][]Condition{
				{
					{0, newExpr(0, types.Type{Oid: types.T_int8})},
				},
				{
					{0, newExpr(0, types.Type{Oid: types.T_int8})},
				},
			}),
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

func TestJoin(t *testing.T) {
	for _, tc := range tcs {
		Prepare(tc.proc, tc.arg)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			if ok, err := Call(tc.proc, tc.arg); ok || err != nil {
				break
			}
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
		}
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp))
	}
}

func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: &plan.Type{
			Size:  typ.Size,
			Scale: typ.Scale,
			Width: typ.Width,
			Id:    plan.Type_TypeId(typ.Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func newTestCase(m *mheap.Mheap, flgs []bool, ts []types.Type, rp []ResultPos, cs [][]Condition) joinTestCase {
	proc := process.New(m)
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 10),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 3),
	}
	return joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &Argument{
			Result:     rp,
			Conditions: cs,
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
