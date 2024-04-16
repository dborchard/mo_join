package transfer

import (
	"bytes"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("transfer")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	reg := n.Reg
	if reg.Ch == nil {
		if proc.Reg.BatchRead != nil {
			bat := proc.Reg.BatchRead.(*batch.Batch)
			bat.Clean(proc)
		}
		return true, nil
	}
	reg.Wg.Add(1)
	reg.Ch <- proc.Reg.BatchRead
	reg.Wg.Wait()
	return false, nil
}
