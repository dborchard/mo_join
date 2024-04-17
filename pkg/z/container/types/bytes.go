package types

import "bytes"

type Bytes struct {
	Data    []byte
	Offsets []uint32
	Lengths []uint32
}

func (a *Bytes) Get(n int64) []byte {
	offset := a.Offsets[n]
	return a.Data[offset : offset+a.Lengths[n]]
}

func (a *Bytes) String() string {
	var buf bytes.Buffer

	buf.WriteByte('[')
	j := len(a.Offsets) - 1
	for i, o := range a.Offsets {
		buf.Write(a.Data[o : o+a.Lengths[i]])
		if i != j {
			buf.WriteByte(' ')
		}
	}
	buf.WriteByte(']')
	return buf.String()
}
