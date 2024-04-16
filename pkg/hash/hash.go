package hash

import (
	"unsafe"
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
	return memhash(noescape(p), seed, s)
}

func F64hash(p unsafe.Pointer, h uintptr) uintptr {
	return f64hash(noescape(p), h)
}
