package inner

import (
	"bytes"
	"fmt"
	"mo_join/pkg/hash"
	"mo_join/pkg/intmap/fastmap"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/vector"
)

func init() {
	ZeroBools = make([]bool, UnitLimit)
	OneUint64s = make([]uint64, UnitLimit)
	for i := range OneUint64s {
		OneUint64s[i] = 1
	}
}

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("%s ⨝ %s", n.R, n.S))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = Container{
		builded: false,
		diffs:   make([]bool, UnitLimit),
		matchs:  make([]int64, UnitLimit),
		hashs:   make([]uint64, UnitLimit),
		sels:    make([][]int64, UnitLimit),
		groups:  make(map[uint64][]*hash.BagGroup),
		slots:   fastmap.Pool.Get().(*fastmap.Map),
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	if !ctr.builded {
		if err := ctr.build(n.Attrs, proc); err != nil {
			return true, err
		}
		ctr.builded = true
	}
	return ctr.probe(n.R, n.S, n.Attrs, proc)
}

// R ⨝ S - S is the smaller relation
func (container *Container) build(attrs []string, proc *process.Process) error {
	var err error

	reg := proc.Reg.WaitRegisters[1]
	for {
		v := <-reg.Ch
		if v == nil {
			reg.Wg.Done()
			break
		}
		bat := v.(*batch.Batch)
		if bat.Attrs == nil {
			reg.Wg.Done()
			continue
		}
		bat.Reorder(attrs)
		if err = bat.Prefetch(attrs, bat.Vecs, proc); err != nil {
			container.clean(bat, proc)
			reg.Wg.Done()
			return err
		}
		container.bats = append(container.bats, bat)
		if len(bat.Sels) == 0 {
			if err = container.buildBatch(bat.Vecs[:len(attrs)], proc); err != nil {
				container.clean(bat, proc)
				reg.Wg.Done()
				return err
			}
		} else {
			if err = container.buildBatchSels(bat.Sels, bat.Vecs[:len(attrs)], proc); err != nil {
				container.clean(bat, proc)
				reg.Wg.Done()
				return err
			}
		}
		reg.Wg.Done()
	}
	return nil
}

func (container *Container) probe(rName, sName string, attrs []string, proc *process.Process) (bool, error) {
	for {
		reg := proc.Reg.WaitRegisters[0]
		v := <-reg.Ch
		if v == nil {
			reg.Wg.Done()
			proc.Reg.NextBatch = nil
			container.clean(nil, proc)
			return true, nil
		}
		bat := v.(*batch.Batch)
		if bat.Attrs == nil {
			reg.Wg.Done()
			continue
		}
		if len(container.groups) == 0 {
			reg.Ch = nil
			reg.Wg.Done()
			proc.Reg.NextBatch = nil
			container.clean(bat, proc)
			return true, nil
		}
		bat.Reorder(attrs)
		if len(container.attrs) == 0 {
			container.attrs = make([]string, 0, len(bat.Attrs)+len(container.bats[0].Attrs))
			for _, attr := range bat.Attrs {
				container.attrs = append(container.attrs, rName+"."+attr)
			}
			for _, attr := range container.bats[0].Attrs {
				container.attrs = append(container.attrs, sName+"."+attr)
			}
		}
		container.probeState.bat = batch.New(true, container.attrs)
		{
			i := 0
			// R relation
			for _, vec := range bat.Vecs {
				container.probeState.bat.Vecs[i] = vector.New(vec.Typ)
				i++
			}
			// S relation
			for _, vec := range container.bats[0].Vecs {
				container.probeState.bat.Vecs[i] = vector.New(vec.Typ)
				i++
			}
		}
		if len(bat.Sels) == 0 {
			if err := container.probeBatch(bat, bat.Vecs[:len(attrs)], proc); err != nil {
				reg.Wg.Done()
				container.clean(bat, proc)
				return true, err
			}
		} else {
			if err := container.probeBatchSels(bat.Sels, bat, bat.Vecs[:len(attrs)], proc); err != nil {
				reg.Wg.Done()
				container.clean(bat, proc)
				return true, err
			}
		}
		if container.probeState.bat.Vecs[0] == nil {
			reg.Wg.Done()
			bat.Clean(proc)
			continue
		}
		reg.Wg.Done()
		bat.Clean(proc)
		proc.Reg.NextBatch = container.probeState.bat
		container.probeState.bat = nil
		return false, nil
	}
}

func (container *Container) buildBatch(vecs []*vector.Vector, proc *process.Process) error {
	rowCount := vecs[0].Length()
	for i, j := 0, rowCount; i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := container.buildUnit(i, length, nil, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (container *Container) buildBatchSels(sels []int64, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, len(sels); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := container.buildUnit(0, length, sels[i:i+length], vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (container *Container) buildUnit(
	start, count int, sels []int64,
	vecs []*vector.Vector, proc *process.Process) error {

	var err error

	{
		copy(container.hashs[:count], OneUint64s[:count])
		if len(sels) == 0 {
			container.fillHash(start, count, vecs)
		} else {
			container.fillHashSels(count, sels, vecs)
		}
	}

	copy(container.diffs[:count], ZeroBools[:count])
	for i, hs := range container.slots.Ks {
		for j, h := range hs {
			remaining := container.sels[container.slots.Vs[i][j]]
			if gs, ok := container.groups[h]; ok {
				for _, g := range gs {
					if remaining, err = g.Fill(remaining, container.matchs, vecs, container.bats, container.diffs, proc); err != nil {
						return err
					}
					copy(container.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			} else {
				container.groups[h] = make([]*hash.BagGroup, 0, 8)
			}
			for len(remaining) > 0 {
				g := hash.NewBagGroup(int64(len(container.bats)-1), int64(remaining[0]))
				container.groups[h] = append(container.groups[h], g)
				if remaining, err = g.Fill(remaining, container.matchs, vecs, container.bats, container.diffs, proc); err != nil {
					return err
				}
				copy(container.diffs[:len(remaining)], ZeroBools[:len(remaining)])
			}
			container.sels[container.slots.Vs[i][j]] = container.sels[container.slots.Vs[i][j]][:0]
		}
	}
	container.slots.Reset()
	return nil
}

func (container *Container) probeBatch(bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}

		if err := container.probeUnit(i, length, nil, bat, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (container *Container) probeBatchSels(sels []int64, bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, len(sels); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := container.probeUnit(0, length, sels[i:i+length], bat, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (container *Container) probeUnit(start, count int, sels []int64, bat *batch.Batch,
	vecs []*vector.Vector, proc *process.Process) error {
	var err error
	var matchs []int64

	{
		copy(container.hashs[:count], OneUint64s[:count])
		if len(sels) == 0 {
			container.fillHash(start, count, vecs)
		} else {
			container.fillHashSels(count, sels, vecs)
		}
	}
	copy(container.diffs[:count], ZeroBools[:count])
	for i, hs := range container.slots.Ks {
		for j, h := range hs {
			remaining := container.sels[container.slots.Vs[i][j]]
			if gs, ok := container.groups[h]; ok {
				for k := 0; k < len(gs); k++ {
					g := gs[k]
					if matchs, remaining, err = g.Probe(remaining, container.matchs, vecs, container.bats, container.diffs, proc); err != nil {
						return err
					}
					if len(matchs) > 0 {
						if err := container.product(len(vecs), matchs, g, bat, proc); err != nil {
							return err
						}
					}
					copy(container.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			}
			container.sels[container.slots.Vs[i][j]] = container.sels[container.slots.Vs[i][j]][:0]
		}
	}
	container.slots.Reset()
	return nil
}

func (container *Container) product(start int, sels []int64, g *hash.BagGroup, bat *batch.Batch, proc *process.Process) error {
	for _, sel := range sels {
		for i, idx := range g.Is {
			{
				for j, vec := range bat.Vecs {
					if container.probeState.bat.Vecs[j].Data == nil {
						if err := container.probeState.bat.Vecs[j].UnionOne(vec, sel, proc); err != nil {
							return err
						}
						copy(container.probeState.bat.Vecs[j].Data[:mempool.HeaderSize], vec.Data[:mempool.HeaderSize])
					} else {
						if err := container.probeState.bat.Vecs[j].UnionOne(vec, sel, proc); err != nil {
							return err
						}
					}
				}
			}
			{
				k := len(bat.Vecs)
				for _, vec := range container.bats[idx].Vecs {
					if container.probeState.bat.Vecs[k].Data == nil {
						if err := container.probeState.bat.Vecs[k].UnionOne(vec, g.Sels[i], proc); err != nil {
							return err
						}
						copy(container.probeState.bat.Vecs[k].Data[:mempool.HeaderSize], vec.Data[:mempool.HeaderSize])
					} else {
						if err := container.probeState.bat.Vecs[k].UnionOne(vec, g.Sels[i], proc); err != nil {
							return err
						}
					}
					k++
				}
			}
		}
	}
	return nil
}

func (container *Container) fillHash(start, count int, vecs []*vector.Vector) {
	container.hashs = container.hashs[:count]
	for _, vec := range vecs {
		hash.Rehash(count, container.hashs, vec)
	}
	nextslot := 0
	for i, h := range container.hashs {
		slot, ok := container.slots.Get(h)
		if !ok {
			slot = nextslot
			container.slots.Set(h, slot)
			nextslot++
		}
		container.sels[slot] = append(container.sels[slot], int64(i+start))
	}
}

func (container *Container) fillHashSels(count int, sels []int64, vecs []*vector.Vector) {
	var cnt int64

	{
		for i, sel := range sels {
			if i == 0 || sel > cnt {
				cnt = sel
			}
		}
	}
	container.hashs = container.hashs[:cnt+1]
	for _, vec := range vecs {
		hash.RehashSels(sels[:count], container.hashs, vec)
	}
	nextslot := 0
	for i, h := range container.hashs {
		slot, ok := container.slots.Get(h)
		if !ok {
			slot = nextslot
			container.slots.Set(h, slot)
			nextslot++
		}
		container.sels[slot] = append(container.sels[slot], sels[i])
	}
}

func (container *Container) clean(bat *batch.Batch, proc *process.Process) {
	if bat != nil {
		bat.Clean(proc)
	}
	fastmap.Pool.Put(container.slots)
	if container.probeState.bat != nil {
		container.probeState.bat.Clean(proc)
	}
	for _, bat := range container.bats {
		bat.Clean(proc)
	}
	for _, gs := range container.groups {
		for _, g := range gs {
			g.Free(proc)
		}
	}
}
