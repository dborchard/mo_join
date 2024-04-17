package indexjoin

import (
	"bytes"
	"mo_join/pkg/vm"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/vector"
)

const argName = "index"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": index join ")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)
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

		case Probe:
			bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return result, err
			}
			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			if arg.buf != nil {
				proc.PutBatch(arg.buf)
				arg.buf = nil
			}
			arg.buf = batch.NewWithSize(len(ap.Result))
			for i, pos := range ap.Result {
				srcVec := bat.Vecs[pos]
				vec := proc.GetVector(*srcVec.GetType())
				if err := vector.GetUnionAllFunction(*srcVec.GetType(), proc.Mp())(vec, srcVec); err != nil {
					vec.Free(proc.Mp())
					return result, err
				}
				arg.buf.SetVector(int32(i), vec)
			}
			arg.buf.AddRowCount(bat.RowCount())
			proc.PutBatch(bat)
			result.Batch = arg.buf
			anal.Output(arg.buf, arg.GetIsLast())
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}
