package plan

type Node struct {
	NodeType Node_NodeType `protobuf:"varint,1,opt,name=node_type,json=nodeType,proto3,enum=plan.Node_NodeType" json:"node_type,omitempty"`
	NodeId   int32         `protobuf:"varint,2,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	Children []int32       `protobuf:"varint,4,rep,packed,name=children,proto3" json:"children,omitempty"`
	// PROJECT
	ProjectList []*Expr `protobuf:"bytes,5,rep,name=project_list,json=projectList,proto3" json:"project_list,omitempty"`
	// JOIN
	JoinType    Node_JoinType `protobuf:"varint,6,opt,name=join_type,json=joinType,proto3,enum=plan.Node_JoinType" json:"join_type,omitempty"`
	OnList      []*Expr       `protobuf:"bytes,7,rep,name=on_list,json=onList,proto3" json:"on_list,omitempty"`
	BuildOnLeft bool          `protobuf:"varint,8,opt,name=build_on_left,json=buildOnLeft,proto3" json:"build_on_left,omitempty"`
	// FILTER
	FilterList []*Expr `protobuf:"bytes,9,rep,name=filter_list,json=filterList,proto3" json:"filter_list,omitempty"`
	// AGG
	GroupBy     []*Expr `protobuf:"bytes,10,rep,name=group_by,json=groupBy,proto3" json:"group_by,omitempty"`
	GroupingSet []*Expr `protobuf:"bytes,11,rep,name=grouping_set,json=groupingSet,proto3" json:"grouping_set,omitempty"`
	AggList     []*Expr `protobuf:"bytes,12,rep,name=agg_list,json=aggList,proto3" json:"agg_list,omitempty"`
	// WINDOW
	WinSpecList []*Expr `protobuf:"bytes,13,rep,name=win_spec_list,json=winSpecList,proto3" json:"win_spec_list,omitempty"`
	// LIMIT
	Limit        *Expr      `protobuf:"bytes,15,opt,name=limit,proto3" json:"limit,omitempty"`
	Offset       *Expr      `protobuf:"bytes,16,opt,name=offset,proto3" json:"offset,omitempty"`
	TableDef     *TableDef  `protobuf:"bytes,17,opt,name=table_def,json=tableDef,proto3" json:"table_def,omitempty"`
	ObjRef       *ObjectRef `protobuf:"bytes,18,opt,name=obj_ref,json=objRef,proto3" json:"obj_ref,omitempty"`
	ParentObjRef *ObjectRef `protobuf:"bytes,19,opt,name=parent_obj_ref,json=parentObjRef,proto3" json:"parent_obj_ref,omitempty"`
	BindingTags  []int32    `protobuf:"varint,24,rep,packed,name=binding_tags,json=bindingTags,proto3" json:"binding_tags,omitempty"`
}
