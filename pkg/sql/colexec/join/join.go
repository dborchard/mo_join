package join

import (
	"bytes"
	"mo_join/pkg/common/hashmap"
	"mo_join/pkg/sql/colexec"
	"mo_join/pkg/vm"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/vector"
)

const argName = "join"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": inner join ")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions[0]))
	ap.ctr.evecs = make([]evalVector, len(ap.Conditions[0]))
	for i := range ap.ctr.evecs {
		ap.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, ap.Conditions[0][i])
		if err != nil {
			return err
		}
	}

	if ap.Cond != nil {
		ap.ctr.expr, err = colexec.NewExpressionExecutor(proc, ap.Cond)
	}
	return err
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ap := arg
	ctr := ap.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(proc, anal); err != nil {
				return result, err
			}
			if ctr.mp == nil && !arg.IsShuffle {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				// shuffle join can't stop early for this moment
				ctr.state = End
			} else {
				ctr.state = Probe
			}
		case Probe:
			if ap.bat == nil {
				bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
				if err != nil {
					return result, err
				}
				if bat == nil {
					ctr.state = End
					continue
				}
				if bat.Last() {
					result.Batch = bat
					return result, nil
				}
				if bat.IsEmpty() {
					proc.PutBatch(bat)
					continue
				}
				if ctr.mp == nil {
					proc.PutBatch(bat)
					continue
				}
				ap.bat = bat
				ap.lastpos = 0
			}

			if err := ctr.probe(ap, proc, anal, arg.GetIsFirst(), arg.GetIsLast(), &result); err != nil {
				proc.PutBatch(ap.bat)
				return result, err
			}
			if ap.lastpos == 0 && ap.count == 0 && ap.sel == 0 {
				proc.PutBatch(ap.bat)
				ap.bat = nil
			}
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) build(proc *process.Process, anal process.Analyze) error {
	err := ctr.receiveHashMap(proc, anal)
	if err != nil {
		return err
	}
	return ctr.receiveBatch(proc, anal)
}

func (ctr *container) receiveHashMap(proc *process.Process, anal process.Analyze) error {
	bat, _, err := ctr.ReceiveFromSingleReg(1, anal)
	if err != nil {
		return err
	}
	if bat != nil && bat.AuxData != nil {
		ctr.mp = bat.DupJmAuxData()
		anal.Alloc(ctr.mp.Size())
	}
	return nil
}

func (ctr *container) receiveBatch(proc *process.Process, anal process.Analyze) error {
	for {
		bat, _, err := ctr.ReceiveFromSingleReg(1, anal)
		if err != nil {
			return err
		}
		if bat != nil {
			ctr.batchRowCount += bat.RowCount()
			ctr.batches = append(ctr.batches, bat)
		} else {
			break
		}
	}
	for i := 0; i < len(ctr.batches)-1; i++ {
		if ctr.batches[i].RowCount() != colexec.DefaultBatchSize {
			panic("wrong batch received for hash build!")
		}
	}
	return nil
}

func (ctr *container) probe(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {

	//Step 1: Initialization and Input Handling
	anal.Input(ap.bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*ap.bat.Vecs[rp.Pos].GetType())
			// for inner join, if left batch is sorted , then output batch is sorted
			ctr.rbat.Vecs[i].SetSorted(ap.bat.Vecs[rp.Pos].GetSorted())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.batches[0].Vecs[rp.Pos].GetType())
		}
	}

	//Step 2: Join Condition Setup
	if err := ctr.evalJoinCondition(ap.bat, proc); err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(ap.bat, proc.Mp())
	}
	if ctr.joinBat2 == nil && ctr.batchRowCount > 0 {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}

	// Step 3: Iterating Batches
	mSels := ctr.mp.Sels()
	count := ap.bat.RowCount()
	itr := ctr.mp.NewIterator()
	rowCount := 0
	for i := ap.lastpos; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		copy(ctr.inBuckets, hashmap.OneUInt8s)

		// Step 4: Find Matching Values
		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)
		k := 0
		if i == ap.lastpos {
			k = ap.count
		}

		// Step 5:  Filter Results
		for ; k < n; k++ {
			if ctr.inBuckets[k] == 0 || zvals[k] == 0 || vals[k] == 0 {
				continue
			}

			idx := vals[k] - 1

			// Step 6: Evaluate Additional Conditions and Build Result
			if ap.HashOnPK {
				if err := ctr.evalApCondForOneSel(ap.bat, ctr.rbat, ap, proc, int64(i+k), int64(idx)); err != nil {
					return err
				}
				rowCount++
			} else {
				sels := mSels[idx][ap.sel:]
				lensels := len(sels)
				if lensels > colexec.DefaultBatchSize {
					sels = sels[:colexec.DefaultBatchSize]
					ap.lastpos = i
					ap.count = k
					ap.sel += colexec.DefaultBatchSize
				} else {
					ap.sel = 0
				}
				for _, sel := range sels {
					if err := ctr.evalApCondForOneSel(ap.bat, ctr.rbat, ap, proc, int64(i+k), int64(sel)); err != nil {
						return err
					}
				}
				rowCount += len(sels)
				if lensels > colexec.DefaultBatchSize {
					ctr.rbat.AddRowCount(rowCount)
					anal.Output(ctr.rbat, isLast)
					result.Batch = ctr.rbat
					return nil
				}
			}
		}
	}
	ctr.rbat.AddRowCount(rowCount)
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	ap.lastpos = 0
	ap.count = 0
	ap.sel = 0
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.evecs {
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}

func (ctr *container) evalApCondForOneSel(bat, rbat *batch.Batch, ap *Argument, proc *process.Process, row, sel int64) error {
	if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, row,
		1, ctr.cfs1); err != nil {
		return err
	}
	idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
	if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], idx2,
		1, ctr.cfs2); err != nil {
		return err
	}
	vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2})
	if err != nil {
		rbat.Clean(proc.Mp())
		return err
	}
	if vec.IsConstNull() {
		return nil
	}
	bs := vector.MustFixedCol[bool](vec)
	if !bs[0] {
		return nil
	}
	for j, rp := range ap.Result {
		if rp.Rel == 0 {
			if err := rbat.Vecs[j].UnionOne(bat.Vecs[rp.Pos], row, proc.Mp()); err != nil {
				rbat.Clean(proc.Mp())
				return err
			}
		} else {
			if err := rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], idx2, proc.Mp()); err != nil {
				rbat.Clean(proc.Mp())
				return err
			}
		}
	}
	return nil
}
