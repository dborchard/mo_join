package nulls

import (
	"github.com/pilosa/pilosa/roaring"
)

type Nulls struct {
	Np *roaring.Bitmap
}

// Contains returns true if the integer is contained in the Nulls
func Contains(n *Nulls, row uint64) bool {
	if n.Np != nil {
		return n.Np.Contains(row)
	}
	return false
}

func Add(n *Nulls, rows ...uint64) {
	if n.Np == nil {
		n.Np = roaring.NewBitmap(rows...)
		return
	}
	n.Np.AddN(rows...)
}

// Any returns true if any bit in the Nulls is set, otherwise it will return false.
func Any(n *Nulls) bool {
	if n.Np == nil {
		return false
	}
	return !n.Np.Any()
}

func RemoveRange(n *Nulls, start, end uint64) {
	if n.Np != nil {
		for i := start; i < end; i++ {
			_, _ = n.Np.Remove(i)
		}
	}
}
