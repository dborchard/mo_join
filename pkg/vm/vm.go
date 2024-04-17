package vm

import (
	"bytes"
	"errors"
	"fmt"
	"mo_join/pkg/vm/process"
)

// String range instructions and call each operator's string function to show a query plan
func String(ins Instructions, buf *bytes.Buffer) {
	for i, in := range ins {
		if i > 0 {
			buf.WriteString(" -> ")
		}
		stringFunc[in.Op](in.Arg, buf)
	}
}

// Prepare range instructions and do init work for each operator's argument by calling its prepare function
func Prepare(ins Instructions, proc *process.Process) error {
	for _, in := range ins {
		if err := prepareFunc[in.Op](proc, in.Arg); err != nil {
			return err
		}
	}
	return nil
}

func Run(ins Instructions, proc *process.Process) (bool, error) {
	var ok bool
	var end bool
	var err error

	defer func() {
		if e := recover(); e != nil {
			err = errors.New(fmt.Sprintf("%v", e))
		}
	}()
	for _, in := range ins {
		if ok, err = execFunc[in.Op](proc, in.Arg); err != nil {
			return ok || end, err
		}
		if ok { // ok is true shows that at least one operator has done its work
			end = true
		}
	}
	return end, err
}
