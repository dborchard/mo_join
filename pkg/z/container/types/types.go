package types

import "errors"

type T uint8

const (
	T_int8 T = iota
)

type Type struct {
	Oid T

	Size      int32 // e.g. int8.Size = 1, int16.Size = 2, char.Size = 24(SliceHeader size)
	Width     int32
	Precision int32
	Scale     int32
}

func (t T) FixedLength() int {
	switch t {

	case T_int8:
		return 1

	}
	panic(errors.New("unknown type %s"))
}
