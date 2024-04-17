package vm

import (
	"bytes"
	"mo_join/pkg/sql/colexec/connector"
	"mo_join/pkg/sql/colexec/join"
	"mo_join/pkg/vm/process"
)

var stringFunc = [...]func(interface{}, *bytes.Buffer){
	Join:      join.String,
	Connector: connector.String,
}

var prepareFunc = [...]func(*process.Process, interface{}) error{
	Join:      join.Prepare,
	Connector: connector.Prepare,
}

var execFunc = [...]func(*process.Process, interface{}) (bool, error){
	Join:      join.Call,
	Connector: connector.Call,
}
