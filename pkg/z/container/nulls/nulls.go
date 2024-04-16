package nulls

import (
	"bytes"
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
