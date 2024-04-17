package types

import "C"
import (
	"math"
	"unsafe"
)

type Decimal64 int64

type Decimal128 struct {
	Lo int64
	Hi int64
}

func AlignDecimal64UsingScaleDiffBatch(src, dst []Decimal64, scaleDiff int32) []Decimal64 {
	scale := int64(math.Pow10(int(scaleDiff)))
	length := len(src)
	for i := 0; i < length; i++ {
		dst[i] = Decimal64(int64(src[i]) * scale)
	}
	return dst
}

// void align_int128_using_scale_diff(void* src, void* dst, void* length, void* scale_diff) {
func AlignDecimal128UsingScaleDiffBatch(src, dst []Decimal128, scaleDiff int32) {
	length := int64(len(src))
	C.align_int128_using_scale_diff(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), unsafe.Pointer(&length), unsafe.Pointer(&scaleDiff))
	return
}
