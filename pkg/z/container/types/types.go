package types

type T uint8

const (
	TFloat64 T = 0
	TVarchar   = 1
)

type Type struct {
	Oid T

	Size      int32 // e.g. int8.Size = 1, int16.Size = 2, char.Size = 24(SliceHeader size)
	Width     int32
	Precision int32
}
