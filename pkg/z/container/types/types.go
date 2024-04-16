package types

type T uint8

const (
	T_any   T = 0
	T_int8    = 1
	T_int16   = 2
)

type Type struct {
	Oid T
}
