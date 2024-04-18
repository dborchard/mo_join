package plan

var _ isExpr_Expr = new(Expr_Col)
var _ isExpr_Expr = new(Expr_F)

func (m *Expr_Col) isExpr_Expr() {}
func (m *Expr_Col) MarshalTo(bytes []byte) (int, error) {
	panic("")
}
func (m *Expr_Col) ProtoSize() (n int) {
	panic("")
}

func (*Expr_F) isExpr_Expr() {}
func (f *Expr_F) MarshalTo(bytes []byte) (int, error) {
	panic("implement me")
}
func (f *Expr_F) ProtoSize() int {
	panic("implement me")
}
