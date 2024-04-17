package nulls

import (
	"bytes"
	"fmt"
	"github.com/pilosa/pilosa/roaring"
)

type Nulls struct {
	Np *roaring.Bitmap
}

func (n *Nulls) Read(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	n.Np = roaring.NewBitmap()
	if err := n.Np.UnmarshalBinary(data); err != nil {
		n.Np = nil
		return err
	}
	return nil
}

func (n *Nulls) Show() ([]byte, error) {
	var buf bytes.Buffer

	if n.Np == nil {
		return nil, nil
	}
	if _, err := n.Np.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (n *Nulls) Add(rows ...uint64) {
	if n.Np == nil {
		n.Np = roaring.NewBitmap(rows...)
		return
	}
	n.Np.DirectAddN(rows...)
}

func (n *Nulls) Contains(row uint64) bool {
	if n.Np != nil {
		return n.Np.Contains(row)
	}
	return false
}

func (n *Nulls) Any() bool {
	if n.Np == nil {
		return false
	}
	return n.Np.Any()
}

func (n *Nulls) String() string {
	if n.Np == nil {
		return "[]"
	}
	return fmt.Sprintf("%v", n.Np.Slice())
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
