package encoding

import (
	"bytes"
	"encoding/gob"
	"mo_join/pkg/z/container/types"
	"reflect"
	"unsafe"
)

var TypeSize int

func init() {
	TypeSize = int(unsafe.Sizeof(types.Type{}))
}

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

func EncodeUint64(v uint64) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 8, Cap: 8}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeUint32(v []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&v[0]))
}

func DecodeInt32(v []byte) int32 {
	return *(*int32)(unsafe.Pointer(&v[0]))
}

func DecodeType(v []byte) types.Type {
	return *(*types.Type)(unsafe.Pointer(&v[0]))
}

func DecodeFloat64Slice(v []byte) []float64 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 8
	hp.Cap /= 8
	return *(*[]float64)(unsafe.Pointer(&hp))
}
func DecodeUint32Slice(v []byte) []uint32 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 4
	hp.Cap /= 4
	return *(*[]uint32)(unsafe.Pointer(&hp))
}

func EncodeInt32(v int32) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 4, Cap: 4}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func EncodeType(v types.Type) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: TypeSize, Cap: TypeSize}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func EncodeFloat64Slice(v []float64) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 8
	hp.Cap *= 8
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func EncodeUint32(v uint32) []byte {
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&v)), Len: 4, Cap: 4}
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func EncodeUint32Slice(v []uint32) []byte {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len *= 4
	hp.Cap *= 4
	return *(*[]byte)(unsafe.Pointer(&hp))
}

func DecodeInt64Slice(v []byte) []int64 {
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v))
	hp.Len /= 8
	hp.Cap /= 8
	return *(*[]int64)(unsafe.Pointer(&hp))
}
