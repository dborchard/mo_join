package compile

import (
	"mo_join/pkg/sql/plan"
	"mo_join/pkg/vm"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
)

// Compile contains all the information needed for compilation.
type Compile struct {
	scope *Scope
	u     interface{}
	//fill is a result writer runs a callback function.
	//fill will be called when result data is ready.
	fill func(interface{}, *batch.Batch) error
	//affectRows stores the number of rows affected while insert / update / delete
	affectRows uint64
	// db current database name.
	db string
	// uid the user who initiated the sql.
	uid string
	// sql sql text.
	sql string
	// e db engine instance.
	e engine.Engine
	// proc stores the execution context.
	proc *process.Process
}

// Scope is the output of the compile process.
// Each sql will be compiled to one or more execution unit scopes.
type Scope struct {
	// Magic specifies the type of Scope.
	// 0 -  execution unit for reading data.
	// 1 -  execution unit for processing intermediate results.
	// 2 -  execution unit that requires remote call.
	Magic int

	// used for dispatch
	DispatchAll bool

	Plan *plan.Plan
	// DataSource stores information about data source.
	DataSource *Source
	// PreScopes contains children of this scope will inherit and execute.
	PreScopes []*Scope
	// NodeInfo contains the information about the remote node.
	NodeInfo engine.Node
	// Instructions contains command list of this scope.
	Instructions vm.Instructions
	// Proc contains the execution context.
	Proc *process.Process

	Reg *process.WaitRegister
}
