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
	Idx   int64
	Sel   int64
	Idata []byte
	Sdata []byte
	Is    []int64
	Sels  []int64
}

func NewBagGroup(idx, sel int64) *BagGroup {
	return &BagGroup{
		Idx: idx,
		Sel: sel,
	}
}

func (g *BagGroup) Free(proc *process.Process) {
	if g.Idata != nil {
		proc.Free(g.Idata)
		g.Idata = nil
	}
	if g.Sdata != nil {
		proc.Free(g.Sdata)
		g.Sdata = nil
	}
}

func (g *BagGroup) Probe(sels, matched []int64, vecs []*vector.Vector,
	bats []*batch.Batch, diffs []bool, proc *process.Process) ([]int64, []int64, error) {
	for i, vec := range vecs {
		switch vec.Typ.Oid {

		case types.T_float64:
			gvec := bats[g.Idx].Vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]float64)
				gv := gvec.Col.([]float64)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]float64)
				gv := gvec.Col.([]float64)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}

		case types.T_varchar:
			gvec := bats[g.Idx].Vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(int(g.Sel))
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(int(g.Sel))
				for i, sel := range sels {
					diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
				}
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

func (g *BagGroup) Fill(sels, matched []int64, vecs []*vector.Vector,
	bats []*batch.Batch, diffs []bool, proc *process.Process) ([]int64, error) {
	for i, vec := range vecs {
		switch vec.Typ.Oid {

		case types.T_float64:
			gvec := bats[g.Idx].Vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]float64)
				gv := gvec.Col.([]float64)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]float64)
				gv := gvec.Col.([]float64)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}

		case types.T_varchar:
			gvec := bats[g.Idx].Vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(int(g.Sel))
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(int(g.Sel))
				for i, sel := range sels {
					diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
				}
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
	if len(matched) > 0 {
		idx := int64(len(bats) - 1)
		length := len(g.Sels) + len(matched)
		if cap(g.Sels) < length {
			iData, err := proc.Alloc(int64(length) * 8)
			if err != nil {
				return nil, err
			}
			sData, err := proc.Alloc(int64(length) * 8)
			if err != nil {
				proc.Free(iData)
				return nil, err
			}
			if g.Idata != nil {
				copy(iData[mempool.HeaderSize:], g.Idata[mempool.HeaderSize:])
				proc.Free(g.Idata)
			}
			if g.Sdata != nil {
				copy(sData[mempool.HeaderSize:], g.Sdata[mempool.HeaderSize:])
				proc.Free(g.Sdata)
			}
			g.Is = encoding.DecodeInt64Slice(iData[mempool.HeaderSize:])
			g.Idata = iData
			g.Is = g.Is[:length-len(matched)]
			g.Sels = encoding.DecodeInt64Slice(sData[mempool.HeaderSize:])
			g.Sdata = sData
			g.Sels = g.Sels[:length-len(matched)]
		}
		g.Sels = append(g.Sels, matched...)
		for range matched {
			g.Is = append(g.Is, idx)
		}
	}
	return remaining, nil
}
