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

package aggregategroup

import (
	"golang.org/x/exp/constraints"

	"bytes"
	"fmt"
	"mo_join/pkg/sql/colexec"
	"mo_join/pkg/sql/colexec/aggregate"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/hashtable"
	"mo_join/pkg/z/container/nulls"
	"mo_join/pkg/z/container/ring"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/container/vector"
	"unsafe"
)

func String(arg interface{}, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("γ([")
	for i, expr := range ap.Exprs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v", expr))
	}
	buf.WriteString("], [")
	for i, agg := range ap.Aggs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v(%v)", aggregate.Names[agg.Op], agg.E))
	}
	buf.WriteString("])")
}

func Prepare(_ *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(Container)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	ap := arg.(*Argument)
	return ap.ctr.processWithGroup(ap, proc)
}

func (ctr *Container) processWithGroup(ap *Argument, proc *process.Process) (bool, error) {
	var err error

	bat := proc.Reg.InputBatch
	if bat == nil {
		if ctr.bat != nil {
			switch ctr.typ {
			case H8:
				ctr.bat.Ht = ctr.intHashMap
			default:
				panic("not implemented")
			}
			proc.Reg.InputBatch = ctr.bat
			ctr.bat = nil
		}
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	defer bat.Clean(proc.Mp)
	proc.Reg.InputBatch = &batch.Batch{}
	if len(ctr.aggVecs) == 0 {
		ctr.aggVecs = make([]evalVector, len(ap.Aggs))
	}
	for i, agg := range ap.Aggs {
		vec, err := colexec.EvalExpr(bat, proc, agg.E)
		if err != nil {
			for j := 0; j < i; j++ {
				if ctr.aggVecs[j].needFree {
					vector.Clean(ctr.aggVecs[j].vec, proc.Mp)
				}
			}
			return false, err
		}
		ctr.aggVecs[i].vec = vec
		ctr.aggVecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.aggVecs[i].needFree = false
				break
			}
		}
	}
	defer func() {
		for i := range ctr.aggVecs {
			if ctr.aggVecs[i].needFree {
				vector.Clean(ctr.aggVecs[i].vec, proc.Mp)
			}
		}
	}()
	if len(ctr.groupVecs) == 0 {
		ctr.groupVecs = make([]evalVector, len(ap.Exprs))
	}
	for i, expr := range ap.Exprs {
		vec, err := colexec.EvalExpr(bat, proc, expr)
		if err != nil {
			for j := 0; j < i; j++ {
				if ctr.groupVecs[j].needFree {
					vector.Clean(ctr.groupVecs[j].vec, proc.Mp)
				}
			}
			return false, err
		}
		ctr.groupVecs[i].vec = vec
		ctr.groupVecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.groupVecs[i].needFree = false
				break
			}
		}
	}
	defer func() {
		for i := range ctr.groupVecs {
			if ctr.groupVecs[i].needFree {
				vector.Clean(ctr.groupVecs[i].vec, proc.Mp)
			}
		}
	}()
	if ctr.bat == nil {
		size := 0
		ctr.bat = batch.NewWithSize(len(ap.Exprs))
		for i := range ctr.groupVecs {
			vec := ctr.groupVecs[i].vec
			ctr.bat.Vecs[i] = vector.New(vec.Typ)
			switch vec.Typ.Oid {
			case types.T_int8:
				size += 1 + 1
			}
		}
		ctr.bat.Rs = make([]ring.Ring, len(ap.Aggs))
		for i, agg := range ap.Aggs {
			if ctr.bat.Rs[i], err = aggregate.New(agg.Op, agg.Dist, ctr.aggVecs[i].vec.Typ); err != nil {
				return false, err
			}
		}
		ctr.keyOffs = make([]uint32, UnitLimit)
		ctr.zKeyOffs = make([]uint32, UnitLimit)
		ctr.inserted = make([]uint8, UnitLimit)
		ctr.zInserted = make([]uint8, UnitLimit)
		ctr.hashes = make([]uint64, UnitLimit)
		ctr.strHashStates = make([][3]uint64, UnitLimit)
		ctr.values = make([]uint64, UnitLimit)
		ctr.intHashMap = &hashtable.Int64HashMap{}
		switch {
		case size <= 8:
			ctr.typ = H8
			ctr.h8.keys = make([]uint64, UnitLimit)
			ctr.h8.zKeys = make([]uint64, UnitLimit)
			ctr.intHashMap.Init()
		}
	}
	switch ctr.typ {
	//TODO: handling only H8
	case H8:
		err = ctr.processH8(bat, ap, proc)
	default:
		panic("not implemented")
	}
	if err != nil {
		ctr.bat.Clean(proc.Mp)
		ctr.bat = nil
		return false, err
	}
	return false, err
}

func (ctr *Container) processH8(bat *batch.Batch, ap *Argument, proc *process.Process) error {
	count := len(bat.Zs)
	for i := 0; i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h8.keys, ctr.h8.zKeys)
		for _, evec := range ctr.groupVecs {
			vec := evec.vec
			switch typLen := vec.Typ.Oid.FixedLength(); typLen {
			case 1:
				fillGroup[uint8](ctr, vec, ctr.h8.keys, n, 1, i)
			}
		}
		ctr.hashes[0] = 0
		ctr.intHashMap.InsertBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values)
		if err := ctr.batchFill(i, n, bat, ap, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) batchFill(i int, n int, bat *batch.Batch, ap *Argument, proc *process.Process) error {
	cnt := 0
	copy(ctr.inserted[:n], ctr.zInserted[:n])
	for k, v := range ctr.values[:n] {
		if v > ctr.rows {
			ctr.inserted[k] = 1
			ctr.rows++
			cnt++
			ctr.bat.Zs = append(ctr.bat.Zs, 0)
		}
		ai := int64(v) - 1
		ctr.bat.Zs[ai] += bat.Zs[i+k]
	}
	if cnt > 0 {
		for j, vec := range ctr.bat.Vecs {
			if err := vector.UnionBatch(vec, ctr.groupVecs[j].vec, int64(i), cnt, ctr.inserted[:n], proc.Mp); err != nil {
				return err
			}
		}
		for _, r := range ctr.bat.Rs {
			if err := r.Grows(cnt, proc.Mp); err != nil {
				return err
			}
		}
	}
	for j, r := range ctr.bat.Rs {
		r.BatchFill(int64(i), ctr.inserted[:n], ctr.values, bat.Zs, ctr.aggVecs[j].vec)
	}
	return nil
}

func fillGroup[T1, T2 any](ctr *Container, vec *vector.Vector, keys []T2, n int, sz uint32, start int) {
	vs := vector.DecodeFixedCol[T1](vec, int(sz))
	if !nulls.Any(vec.Nsp) {
		for i := 0; i < n; i++ {
			*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i])) = 0
			*(*T1)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i]+1)) = vs[i+start]
		}
		NumericAddScalar(1+sz, ctr.keyOffs[:n], ctr.keyOffs[:n])
	} else {
		for i := 0; i < n; i++ {
			if vec.Nsp.Np.Contains(uint64(i + start)) {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i])) = 1
				ctr.keyOffs[i]++
			} else {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i])) = 0
				*(*T1)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i]+1)) = vs[i+start]
				ctr.keyOffs[i] += 1 + sz
			}
		}
	}
}

func NumericAddScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}
