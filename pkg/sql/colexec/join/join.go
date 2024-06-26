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
	"mo_join/pkg/sql/colexec"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/hashtable"
	"mo_join/pkg/z/container/nulls"
	"mo_join/pkg/z/container/vector"
	"unsafe"
)

func init() {
	OneInt64s = make([]int64, UnitLimit)
	for i := range OneInt64s {
		OneInt64s[i] = 1
	}
}

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(" ⨝ ")
}

func Prepare(proc *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(Container)
	ap.ctr.keys = make([][]byte, UnitLimit)
	ap.ctr.values = make([]uint64, UnitLimit)
	ap.ctr.zValues = make([]int64, UnitLimit)
	ap.ctr.inserted = make([]uint8, UnitLimit)
	ap.ctr.zInserted = make([]uint8, UnitLimit)
	ap.ctr.strHashStates = make([][3]uint64, UnitLimit)
	ap.ctr.strHashMap = &hashtable.StringHashMap{}
	ap.ctr.strHashMap.Init()
	ap.ctr.vecs = make([]evalVector, len(ap.Conditions[0]))
	{
		flg := false
		for _, rp := range ap.Result {
			if rp.Rel == 1 {
				ap.ctr.colPos = append(ap.ctr.colPos, rp.Pos)
				flg = true
			}
		}
		ap.ctr.flg = flg
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc); err != nil {
				ctr.state = End
				return true, err
			}
			ctr.state = Probe
		case Probe:
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = End
				ctr.bat.Clean(proc.Mp)
				continue
			}
			if len(bat.Zs) == 0 {
				continue
			}
			if err := ctr.probe(bat, ap, proc); err != nil {
				ctr.state = End
				proc.Reg.InputBatch = nil
				return true, err
			}
			return false, nil
		default:
			proc.Reg.InputBatch = nil
			return true, nil
		}
	}
}

func (ctr *Container) build(arg *Argument, proc *process.Process) error {
	if arg.IsPreBuild {
		bat := <-proc.Reg.MergeReceivers[1].Ch
		ctr.bat = bat
		ctr.strHashMap = bat.Ht.(*hashtable.StringHashMap)
		return nil
	}

	for {
		bat := <-proc.Reg.MergeReceivers[1].Ch
		if bat == nil {
			return nil
		}
		if len(bat.Zs) == 0 {
			continue
		}
		if ctr.bat == nil {
			ctr.bat = batch.NewWithSize(len(bat.Vecs))
			for _, pos := range ctr.colPos {
				ctr.bat.Vecs[pos] = vector.New(bat.Vecs[pos].Typ)
			}
		}
		for i, cond := range arg.Conditions[1] {
			vec, _ := colexec.EvalExpr(bat, proc, cond.Expr)
			ctr.vecs[i].vec = vec
			ctr.vecs[i].needFree = true
			for j := range bat.Vecs {
				if bat.Vecs[j] == vec {
					ctr.vecs[i].needFree = false
					break
				}
			}
		}
		count := len(bat.Zs)
		for i := 0; i < count; i += UnitLimit {
			n := count - i
			if n > UnitLimit {
				n = UnitLimit
			}
			copy(ctr.zValues[:n], OneInt64s[:n])
			for j := range arg.Conditions[1] {
				vec := ctr.vecs[j].vec
				switch typLen := vec.Typ.Oid.FixedLength(); typLen {
				case 1:
					fillGroupStr[uint8](ctr, vec, n, 1, i)
				case 2:
					fillGroupStr[uint16](ctr, vec, n, 2, i)
				case 4:
					fillGroupStr[uint32](ctr, vec, n, 4, i)
				case 8:
					fillGroupStr[uint64](ctr, vec, n, 8, i)
				default:

				}
			}
			for k := 0; k < n; k++ {
				if l := len(ctr.keys[k]); l < 16 {
					ctr.keys[k] = append(ctr.keys[k], hashtable.StrKeyPadding[l:]...)
				}
			}
			ctr.strHashMap.InsertStringBatchWithRing(ctr.zValues, ctr.strHashStates, ctr.keys[:n], ctr.values)
			cnt := 0
			copy(ctr.inserted[:n], ctr.zInserted[:n])
			for k, v := range ctr.values[:n] {
				if ctr.zValues[k] == 0 {
					continue
				}
				if v > ctr.rows {
					cnt++
					ctr.rows++
					ctr.inserted[k] = 1
					ctr.bat.Zs = append(ctr.bat.Zs, 0)
				}
				ai := int64(v) - 1
				ctr.bat.Zs[ai] += bat.Zs[i+k]
			}
			if cnt > 0 {
				for _, pos := range ctr.colPos {
					if err := vector.UnionBatch(ctr.bat.Vecs[pos], bat.Vecs[pos], int64(i), cnt, ctr.inserted[:n], proc.Mp); err != nil {
						bat.Clean(proc.Mp)
						ctr.bat.Clean(proc.Mp)
						for i := range ctr.vecs {
							if ctr.vecs[i].needFree {
								vector.Clean(ctr.vecs[i].vec, proc.Mp)
							}
						}
						return err
					}

				}
			}
			for k := 0; k < n; k++ {
				ctr.keys[k] = ctr.keys[k][:0]
			}
			bat.Clean(proc.Mp)
		}
		for i := range ctr.vecs {
			if ctr.vecs[i].needFree {
				vector.Clean(ctr.vecs[i].vec, proc.Mp)
			}
		}
	}
}

func (ctr *Container) probe(bat *batch.Batch, arg *Argument, proc *process.Process) error {
	defer bat.Clean(proc.Mp)
	rbat := batch.NewWithSize(len(arg.Result))
	for i, rp := range arg.Result {
		if rp.Rel == 0 {
			rbat.Vecs[i] = vector.New(bat.Vecs[rp.Pos].Typ)
		} else {
			rbat.Vecs[i] = vector.New(ctr.bat.Vecs[rp.Pos].Typ)
		}
	}
	for i, cond := range arg.Conditions[0] {
		vec, _ := colexec.EvalExpr(bat, proc, cond.Expr)
		ctr.vecs[i].vec = vec
		ctr.vecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.vecs[i].needFree = false
				break
			}
		}
	}
	defer func() {
		for i := range ctr.vecs {
			if ctr.vecs[i].needFree {
				vector.Clean(ctr.vecs[i].vec, proc.Mp)
			}
		}
	}()
	count := len(bat.Zs)
	for i := 0; i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.zValues[:n], OneInt64s[:n])
		for j := range arg.Conditions[0] {
			vec := ctr.vecs[j].vec
			switch typLen := vec.Typ.Oid.FixedLength(); typLen {
			case 1:
				fillGroupStr[uint8](ctr, vec, n, 1, i)
			case 2:
				fillGroupStr[uint16](ctr, vec, n, 2, i)
			case 4:
				fillGroupStr[uint32](ctr, vec, n, 4, i)
			case 8:
				fillGroupStr[uint64](ctr, vec, n, 8, i)
			default:
			}
		}
		for k := 0; k < n; k++ {
			if l := len(ctr.keys[k]); l < 16 {
				ctr.keys[k] = append(ctr.keys[k], hashtable.StrKeyPadding[l:]...)
			}
		}
		ctr.strHashMap.FindStringBatch(ctr.strHashStates, ctr.keys[:n], ctr.values)
		for k := 0; k < n; k++ {
			ctr.keys[k] = ctr.keys[k][:0]
		}
		for k := 0; k < n; k++ {
			if ctr.zValues[k] == 0 {
				continue
			}
			if ctr.values[k] == 0 {
				continue
			}

			sel := int64(ctr.values[k] - 1)
			for j, rp := range arg.Result {
				if rp.Rel == 0 {
					if err := vector.UnionOne(rbat.Vecs[j], bat.Vecs[rp.Pos], int64(i+k), proc.Mp); err != nil {
						rbat.Clean(proc.Mp)
						return err
					}
				} else {
					if err := vector.UnionOne(rbat.Vecs[j], ctr.bat.Vecs[rp.Pos], sel, proc.Mp); err != nil {
						rbat.Clean(proc.Mp)
						return err
					}
				}
			}
			rbat.Zs = append(rbat.Zs, ctr.bat.Zs[sel])

		}
	}
	proc.Reg.InputBatch = rbat
	return nil
}

func fillGroupStr[T any](ctr *Container, vec *vector.Vector, n int, sz int, start int) {
	vs := vector.DecodeFixedCol[T](vec, sz)
	data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*sz)[:len(vs)*sz]
	if !nulls.Any(vec.Nsp) {
		for i := 0; i < n; i++ {
			ctr.keys[i] = append(ctr.keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
		}
	} else {
		for i := 0; i < n; i++ {
			if vec.Nsp.Np.Contains(uint64(i + start)) {
				ctr.zValues[i] = 0
			} else {
				ctr.keys[i] = append(ctr.keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
			}
		}
	}
}
