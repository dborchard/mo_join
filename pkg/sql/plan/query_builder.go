package plan

import "mo_join/pkg/z/pb/plan"

func (builder *QueryBuilder) appendNode(node *plan.Node, ctx *BindContext) int32 {
	nodeID := int32(len(builder.qry.Nodes))
	node.NodeId = nodeID
	builder.qry.Nodes = append(builder.qry.Nodes, node)
	builder.ctxByNode = append(builder.ctxByNode, ctx)
	//ReCalcNodeStats(nodeID, builder, false, true, true)
	return nodeID
}
