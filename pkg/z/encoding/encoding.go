package encoding

import (
	"bytes"
	"encoding/gob"
	"errors"
	"mo_join/pkg/z/container/types"
	"unsafe"
)

var TypeSize int

func init() {
	TypeSize = int(unsafe.Sizeof(types.Type{}))
}

func EncodeType(v *types.Type) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), TypeSize)
}

func DecodeType(v []byte) types.Type {
	return *(*types.Type)(unsafe.Pointer(&v[0]))
}

//-----------------------------------------------------------

func Encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func Decode(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

//-----------------------------------------------------------

func EncodeInt32(v *int32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 4)
}

func DecodeInt32(v []byte) int32 {
	return *(*int32)(unsafe.Pointer(&v[0]))
}

func EncodeInt64(v *int64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 8)
}

//-----------------------------------------------------------

func EncodeUint32(v *uint32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 4)
}

func DecodeUint32(v []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&v[0]))
}

//-----------------------------------------------------------

func EncodeSlice[T any](v []T) []byte {
	var t T
	sz := int(unsafe.Sizeof(t))
	if len(v) > 0 {
		return unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*sz)[:len(v)*sz]
	}
	return nil
}

func DecodeSlice[T any](v []byte) []T {
	var t T
	sz := int(unsafe.Sizeof(t))

	if len(v)%sz != 0 {
		panic(errors.New("decode slice that is not a multiple of element size"))
	}

	if len(v) > 0 {
		return unsafe.Slice((*T)(unsafe.Pointer(&v[0])), len(v)/sz)[:len(v)/sz]
	}
	return nil
}

//-----------------------------------------------------------
