package compile

import (
	"fmt"
	"mo_join/pkg/sql/colexec/join"
	"mo_join/pkg/sql/plan"
	"mo_join/pkg/vm/process"
)

func constructJoin(n *plan.Node, proc *process.Process) *join.Argument {
	result := make([]join.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	conds := make([][]join.Condition, 2)
	{
		conds[0] = make([]join.Condition, len(n.OnList))
		conds[1] = make([]join.Condition, len(n.OnList))
	}
	for i, expr := range n.OnList {
		conds[0][i].Expr, conds[1][i].Expr = constructJoinCondition(expr)
	}
	return &join.Argument{
		IsPreBuild: false,
		Conditions: conds,
		Result:     result,
	}
}

func constructJoinResult(expr *plan.Expr) (int32, int32) {
	e, ok := expr.Expr.(*plan.Expr_Col)
	if !ok {
		panic(fmt.Sprintf("join result %v not supported yet", expr))
	}
	return e.Col.RelPos, e.Col.ColPos
}

func constructJoinCondition(expr *plan.Expr) (*plan.Expr, *plan.Expr) {
	e, ok := expr.Expr.(*plan.Expr_F)
	if !ok {
		panic(fmt.Sprintf("join condition '%v' not support now", expr))
	}
	if exprRelPos(e.F.Args[0]) == 1 {
		return e.F.Args[1], e.F.Args[0]
	}
	return e.F.Args[0], e.F.Args[1]
}

func exprRelPos(expr *plan.Expr) int32 {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return e.Col.RelPos
	case *plan.Expr_F:
		for i := range e.F.Args {
			if relPos := exprRelPos(e.F.Args[i]); relPos >= 0 {
				return relPos
			}
		}
	}
	return -1
}
