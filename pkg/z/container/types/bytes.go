package types

type Bytes struct {
	Data    []byte
	Offsets []uint32
	Lengths []uint32
}

func (a *Bytes) Append(vs [][]byte) error {
	o := uint32(len(a.Data))
	for _, v := range vs {
		a.Offsets = append(a.Offsets, o)
		a.Data = append(a.Data, v...)
		o += uint32(len(v))
		a.Lengths = append(a.Lengths, uint32(len(v)))
	}
	return nil
}

func (a *Bytes) Reset() {
	a.Offsets = a.Offsets[:0]
	a.Lengths = a.Lengths[:0]
	a.Data = a.Data[:0]
}

func (a *Bytes) Get(n int) []byte {
	offset := a.Offsets[n]
	return a.Data[offset : offset+a.Lengths[n]]
}
