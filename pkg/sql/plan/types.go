package plan

import (
	"context"
	"mo_join/pkg/z/pb/plan"
)

type QueryBuilder struct {
	qry *plan.Query
}

func (builder *QueryBuilder) GetContext() context.Context {
	return nil
}

type BindContext struct {
}

type Expr = plan.Expr
