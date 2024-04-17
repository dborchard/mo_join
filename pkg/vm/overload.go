package vm

import (
	"bytes"
	"mo_join/pkg/sql/colexec/connector"
	"mo_join/pkg/sql/colexec/join"
	"mo_join/pkg/vm/overload"
	"mo_join/pkg/vm/process"
)

var stringFunc = [...]func(interface{}, *bytes.Buffer){
	overload.Join:      join.String,
	overload.Connector: connector.String,
}

var prepareFunc = [...]func(*process.Process, interface{}) error{
	overload.Join:      join.Prepare,
	overload.Connector: connector.Prepare,
}

var execFunc = [...]func(*process.Process, interface{}) (bool, error){
	overload.Join:      join.Call,
	overload.Connector: connector.Call,
}
