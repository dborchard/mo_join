package vm

import (
	"bytes"
	"mo_join/pkg/vm/process"
)

func Prepare(ins Instructions, proc *process.Process) error {
	for _, in := range ins {
		if err := pFuncs[in.Op](proc, in.Arg); err != nil {
			return err
		}
	}
	return nil
}

func Run(ins Instructions, proc *process.Process) (bool, error) {
	var ok bool
	var end bool
	var err error

	for _, in := range ins {
		ok, err = rFuncs[in.Op](proc, in.Arg)
		if err != nil {
			return false, err
		}
		if ok {
			end = true
		}
	}
	return end, nil
}
func String(ins Instructions, buf *bytes.Buffer) {
	for i, in := range ins {
		if i > 0 {
			buf.WriteString(" -> ")
		}
		sFuncs[in.Op](in.Arg, buf)
	}
}

func Clean(_ Instructions, _ *process.Process) {
}