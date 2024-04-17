package colexec

import (
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"reflect"
	"time"
)

// isMergeType means the receiver operator receive batch from all regs or single by some order
// Merge/MergeGroup/MergeLimit ... are Merge-Type
// while Join/Intersect/Minus ... are not
func (r *ReceiverOperator) InitReceiver(proc *process.Process, isMergeType bool) {
	r.proc = proc
	if isMergeType {
		r.aliveMergeReceiver = len(proc.Reg.MergeReceivers)
		r.chs = make([]chan *batch.Batch, r.aliveMergeReceiver)
		r.receiverListener = make([]reflect.SelectCase, r.aliveMergeReceiver+1)
		r.receiverListener[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(r.proc.Ctx.Done())}
		for i, mr := range proc.Reg.MergeReceivers {
			r.chs[i] = mr.Ch
			r.receiverListener[i+1] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(mr.Ch),
			}
		}
	}
}

func (r *ReceiverOperator) FreeAllReg() {
	for i := range r.proc.Reg.MergeReceivers {
		r.FreeSingleReg(i)
	}
}
func (r *ReceiverOperator) FreeSingleReg(regIdx int) {
	w := r.proc.Reg.MergeReceivers[regIdx]
	w.CleanChannel(r.proc.GetMPool())
}

func (r *ReceiverOperator) ReceiveFromSingleReg(regIdx int, analyze process.Analyze) (*batch.Batch, bool, error) {
	start := time.Now()
	defer analyze.WaitStop(start)
	select {
	case <-r.proc.Ctx.Done():
		return nil, true, nil
	case bat, ok := <-r.proc.Reg.MergeReceivers[regIdx].Ch:
		if !ok {
			return nil, true, nil
		}
		return bat, false, nil
	}
}
