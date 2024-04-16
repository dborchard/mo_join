package vm

import (
	"bytes"
	binner "mo_join/pkg/sql/colexec/bag/inner"
	"mo_join/pkg/vm/process"
)

var sFuncs = [...]func(interface{}, *bytes.Buffer){
	BagInnerJoin: binner.String,
}

var pFuncs = [...]func(*process.Process, interface{}) error{
	BagInnerJoin: binner.Prepare,
}

var rFuncs = [...]func(*process.Process, interface{}) (bool, error){
	BagInnerJoin: binner.Call,
}
