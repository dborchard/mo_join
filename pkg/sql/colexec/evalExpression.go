package colexec

import (
	"mo_join/pkg/common/mpool"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/vector"
	"mo_join/pkg/z/pb/plan"
)

// ExpressionExecutor
// generated from plan.Expr, can evaluate the result from vectors directly.
type ExpressionExecutor interface {
	// Eval will return the result vector of expression.
	// the result memory is reused, so it should not be modified or saved.
	// If it needs, it should be copied by vector.Dup().
	Eval(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error)

	// EvalWithoutResultReusing is the same as Eval, but it will not reuse the memory of result vector.
	// so you can save the result vector directly. but should be careful about memory leak.
	// and watch out that maybe the vector is one of the input vectors of batches.
	EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error)

	// Free should release all memory of executor.
	// it will be called after query has done.
	Free()

	IsColumnExpr() bool
}

func NewExpressionExecutor(proc *process.Process, planExpr *plan.Expr) (ExpressionExecutor, error) {
	return nil, nil
}

func NewJoinBatch(bat *batch.Batch, mp *mpool.MPool) (*batch.Batch, []func(*vector.Vector, *vector.Vector, int64, int) error) {
	rbat := batch.NewWithSize(bat.VectorCount())
	cfs := make([]func(*vector.Vector, *vector.Vector, int64, int) error, bat.VectorCount())
	for i, vec := range bat.Vecs {
		typ := *vec.GetType()
		rbat.Vecs[i] = vector.NewConstNull(typ, 0, nil)
		cfs[i] = vector.GetConstSetFunction(typ, mp)
	}
	return rbat, cfs
}
func SetJoinBatchValues(joinBat, bat *batch.Batch, sel int64, length int,
	cfs []func(*vector.Vector, *vector.Vector, int64, int) error) error {
	for i, vec := range bat.Vecs {
		if err := cfs[i](joinBat.Vecs[i], vec, sel, length); err != nil {
			return err
		}
	}
	joinBat.SetRowCount(length)
	return nil
}
