package hash

import (
	"math"
	"math/rand"
	"unsafe"
)

const (
	ptrSize = 4 << (^uintptr(0) >> 63) // unsafe.Sizeof(uintptr(0)) but an ideal const
	c0      = uintptr((8-ptrSize)/4*2860486313 + (ptrSize-4)/4*33054211828000289)
	c1      = uintptr((8-ptrSize)/4*3267000013 + (ptrSize-4)/4*23344194077549503)
)

// This function is copied from the Go runtime.
// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	//lint:ignore SA4016 x ^ 0 is a no-op that fools escape analysis.
	return unsafe.Pointer(x ^ 0)
}

func Memhash(p unsafe.Pointer, seed, s uintptr) uintptr {
	return uintptr(0)
}

func F64hash(p unsafe.Pointer, h uintptr) uintptr {
	f := *(*float64)(p)
	if math.IsNaN(f) {
		f = 0
	}
	switch {
	case f == 0:
		return c1 * (c0 ^ h) // +0, -0
	case f != f:
		// TODO(asubiotto): fastrand relies on some stack internals.
		//return c1 * (c0 ^ h ^ uintptr(fastrand())) // any kind of NaN
		return c1 * (c0 ^ h ^ uintptr(rand.Uint32())) // any kind of NaN
	default:
		return Memhash(p, h, 8)
	}
}
