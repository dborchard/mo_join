package colexec

import (
	"errors"
	"fmt"
	"mo_join/pkg/sql/plan"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/vector"
)

func EvalExpr(bat *batch.Batch, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_Col:
		return bat.Vecs[t.Col.ColPos], nil
	default:
		return nil, errors.New(fmt.Sprintf("unsupported eval expr '%v'", t))
	}
}
