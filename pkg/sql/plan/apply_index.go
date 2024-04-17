package plan

import (
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/pb/plan"
)

func makeTblCrossJoinEntriesCentroidOnPK(builder *QueryBuilder, bindCtx *BindContext,
	scanNode *plan.Node, entriesJoinCentroids int32) int32 {

	entriesOriginPkEqTblPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: plan.Type{
				Id: int32(types.T_any),
			},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["entries.project"],
					ColPos: 2, // entries.origin_pk
				},
			},
		},
		{
			Typ: plan.Type{
				Id: int32(types.T_any),
			},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: pkPos, // tbl.pk
				},
			},
		},
	})

	entriesJoinTbl := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{entriesJoinCentroids, scanNode.NodeId},
		OnList:   []*Expr{entriesOriginPkEqTblPk},
	}, bindCtx)

	return entriesJoinTbl
}
