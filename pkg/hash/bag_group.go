package hash

import (
	"bytes"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/container/vector"
	"mo_join/pkg/z/encoding"
)

type BagGroup struct {
	BatchIdx int64 // column
	Sel      int64 // row

	Idata []byte
	Sdata []byte

	Is   []int64
	Sels []int64
}

func NewBagGroup(idx, sel int64) *BagGroup {
	return &BagGroup{
		BatchIdx: idx,
		Sel:      sel,
	}
}

func (bagGroup *BagGroup) Fill(sels, matched []int64,
	vecs []*vector.Vector,
	bats []*batch.Batch,
	diffs []bool,
	proc *process.Process) ([]int64, error) {
	for i, vec := range vecs {
		switch vec.Typ.Oid {

		case types.TFloat64:
			currentBatchVec := bats[bagGroup.BatchIdx].Vecs[i]

			vs := vec.Col.([]float64)
			gv := currentBatchVec.Col.([]float64)[bagGroup.Sel]
			for i, sel := range sels {
				diffs[i] = diffs[i] || (gv != vs[sel])
			}

		case types.TVarchar:
			gvec := bats[bagGroup.BatchIdx].Vecs[i]

			vs := vec.Col.(*types.Bytes)
			gvs := gvec.Col.(*types.Bytes)
			gv := gvs.Get(int(bagGroup.Sel))
			for i, sel := range sels {
				diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
			}

		}
	}
	//NOTE: optimization.
	n := len(sels)
	matched = matched[:0]
	remaining := sels[:0]
	for i := 0; i < n; i++ {
		if diffs[i] {
			remaining = append(remaining, sels[i])
		} else {
			matched = append(matched, sels[i])
		}
	}
	if len(matched) > 0 {
		idx := int64(len(bats) - 1)
		length := len(bagGroup.Sels) + len(matched)
		if cap(bagGroup.Sels) < length {
			// Capacity increase.
			iData, err := proc.Alloc(int64(length) * 8)
			if err != nil {
				return nil, err
			}
			sData, err := proc.Alloc(int64(length) * 8)
			if err != nil {
				proc.Free(iData)
				return nil, err
			}
			if bagGroup.Idata != nil {
				copy(iData[mempool.HeaderSize:], bagGroup.Idata[mempool.HeaderSize:])
				proc.Free(bagGroup.Idata)
			}
			if bagGroup.Sdata != nil {
				copy(sData[mempool.HeaderSize:], bagGroup.Sdata[mempool.HeaderSize:])
				proc.Free(bagGroup.Sdata)
			}
			bagGroup.Is = encoding.DecodeSlice[int64](iData[mempool.HeaderSize:])
			bagGroup.Idata = iData
			bagGroup.Is = bagGroup.Is[:length-len(matched)]
			bagGroup.Sels = encoding.DecodeSlice[int64](sData[mempool.HeaderSize:])
			bagGroup.Sdata = sData
			bagGroup.Sels = bagGroup.Sels[:length-len(matched)]
		}
		bagGroup.Sels = append(bagGroup.Sels, matched...)
		for range matched {
			bagGroup.Is = append(bagGroup.Is, idx)
		}
	}
	return remaining, nil
}

func (bagGroup *BagGroup) Probe(sels, matched []int64,
	vecs []*vector.Vector,
	bats []*batch.Batch,
	diffs []bool,
	proc *process.Process) ([]int64, []int64, error) {

	for i, vec := range vecs {
		switch vec.Typ.Oid {

		case types.TFloat64:
			gvec := bats[bagGroup.BatchIdx].Vecs[i]

			vs := vec.Col.([]float64)
			gv := gvec.Col.([]float64)[bagGroup.Sel]
			for i, sel := range sels {
				diffs[i] = diffs[i] || (gv != vs[sel])
			}

		case types.TVarchar:
			gvec := bats[bagGroup.BatchIdx].Vecs[i]

			vs := vec.Col.(*types.Bytes)
			gvs := gvec.Col.(*types.Bytes)
			gv := gvs.Get(int(bagGroup.Sel))
			for i, sel := range sels {
				diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
			}

		}
	}
	n := len(sels)
	matched = matched[:0]
	remaining := sels[:0]
	for i := 0; i < n; i++ {
		if diffs[i] {
			remaining = append(remaining, sels[i])
		} else {
			matched = append(matched, sels[i])
		}
	}
	return matched, remaining, nil
}

func (bagGroup *BagGroup) Free(proc *process.Process) {
	if bagGroup.Idata != nil {
		proc.Free(bagGroup.Idata)
		bagGroup.Idata = nil
	}
	if bagGroup.Sdata != nil {
		proc.Free(bagGroup.Sdata)
		bagGroup.Sdata = nil
	}
}
