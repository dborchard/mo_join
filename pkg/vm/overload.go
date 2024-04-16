package vm

import (
	"bytes"
	binner "mo_join/pkg/sql/colexec/bag/inner"
	"mo_join/pkg/sql/colexec/output"
	"mo_join/pkg/sql/colexec/transfer"
	"mo_join/pkg/vm/process"
)

var sFuncs = [...]func(interface{}, *bytes.Buffer){
	BagInnerJoin: binner.String,
	Transfer:     transfer.String,
	Output:       output.String,
}

var pFuncs = [...]func(*process.Process, interface{}) error{
	BagInnerJoin: binner.Prepare,
	Transfer:     transfer.Prepare,
	Output:       output.Prepare,
}

var rFuncs = [...]func(*process.Process, interface{}) (bool, error){
	BagInnerJoin: binner.Call,
	Transfer:     transfer.Call,
	Output:       output.Call,
}
