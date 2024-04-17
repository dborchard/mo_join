package plan

type Expr struct {
	Typ Type `protobuf:"bytes,1,opt,name=typ,proto3" json:"typ"`
	// Types that are valid to be assigned to Expr:
	//
	//	*Expr_Lit
	//	*Expr_P
	//	*Expr_V
	//	*Expr_Col
	//	*Expr_Raw
	//	*Expr_F
	//	*Expr_W
	//	*Expr_Sub
	//	*Expr_Corr
	//	*Expr_T
	//	*Expr_List
	//	*Expr_Max
	//	*Expr_Vec
	Expr        isExpr_Expr `protobuf_oneof:"expr"`
	AuxId       int32       `protobuf:"varint,15,opt,name=aux_id,json=auxId,proto3" json:"aux_id,omitempty"`
	Ndv         float64     `protobuf:"fixed64,16,opt,name=ndv,proto3" json:"ndv,omitempty"`
	Selectivity float64     `protobuf:"fixed64,17,opt,name=selectivity,proto3" json:"selectivity,omitempty"`
}

type isExpr_Expr interface {
	isExpr_Expr()
	MarshalTo([]byte) (int, error)
	ProtoSize() int
}

type Expr_F struct {
	F *Function
}

type Function struct {
	Func ObjectRef
}

type Expr_Col struct {
	Col *Column
}

func (e *Expr_Col) isExpr_Expr() {
	//TODO implement me
	panic("implement me")
}

func (e *Expr_Col) MarshalTo(bytes []byte) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (e *Expr_Col) ProtoSize() int {
	//TODO implement me
	panic("implement me")
}

type Column struct {
	Name string
}
