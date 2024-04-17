package join

import (
	"mo_join/pkg/common/hashmap"
	"mo_join/pkg/sql/colexec"
	"mo_join/pkg/vm"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/container/vector"
	"mo_join/pkg/z/pb/plan"
)

var _ vm.Operator = new(Argument)

const (
	Build = iota
	Probe
	End
)

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	colexec.ReceiverOperator

	state int

	inBuckets []uint8

	batches       []*batch.Batch
	batchRowCount int
	rbat          *batch.Batch

	expr colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	evecs []evalVector
	vecs  []*vector.Vector

	mp *hashmap.JoinMap

	maxAllocSize int64
}

type Argument struct {
	ctr        *container
	Ibucket    uint64 // index in buckets
	Nbucket    uint64 // buckets count
	Result     []colexec.ResultPos
	Typs       []types.Type
	Cond       *plan.Expr
	Conditions [][]*plan.Expr // join conditions
	bat        *batch.Batch
	lastpos    int
	count      int
	sel        int

	HashOnPK           bool
	IsShuffle          bool
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func (arg *Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return &Argument{}
}

func (arg *Argument) Release() {

}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := arg.ctr
	if ctr != nil {
		ctr.cleanBatch(proc)
		ctr.cleanEvalVectors()
		ctr.cleanHashMap()
		ctr.cleanExprExecutor()
		ctr.FreeAllReg()
	}
}

func (ctr *container) cleanExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.Free()
		ctr.expr = nil
	}
}

func (ctr *container) cleanBatch(proc *process.Process) {
	for i := range ctr.batches {
		proc.PutBatch(ctr.batches[i])
	}
	ctr.batches = nil
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	if ctr.joinBat1 != nil {
		proc.PutBatch(ctr.joinBat1)
		ctr.joinBat1 = nil
	}
	if ctr.joinBat2 != nil {
		proc.PutBatch(ctr.joinBat2)
		ctr.joinBat2 = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEvalVectors() {
	for i := range ctr.evecs {
		if ctr.evecs[i].executor != nil {
			ctr.evecs[i].executor.Free()
		}
		ctr.evecs[i].vec = nil
	}
	ctr.evecs = nil
}
