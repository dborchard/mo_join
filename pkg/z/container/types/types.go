package types

type T uint8

const (
	T_any   T = 0
	T_Rowid T = 101
)

type Type struct {
	Oid T
}
