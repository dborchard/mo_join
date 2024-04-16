package output

import (
	"bytes"
	"fmt"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
)

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("output")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	if proc.Reg.BatchRead != nil {
		bat := proc.Reg.BatchRead.(*batch.Batch)
		fmt.Printf("%s\n", bat)
		bat.Clean(proc)
	}
	return false, nil
}
