package fastmap

const (
	Width     = 4
	Group     = 16
	GroupMask = 0xF
)

type Map struct {
	Vs [][]int
	Ks [][]uint64
}

func (m *Map) Reset() {
	for i := 0; i < Group; i++ {
		m.Ks[i] = m.Ks[i][:0]
		m.Vs[i] = m.Vs[i][:0]
	}
}
