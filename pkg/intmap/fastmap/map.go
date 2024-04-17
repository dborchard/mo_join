package fastmap

import "sync"

var Pool = sync.Pool{
	New: func() interface{} {
		return New()
	},
}

const (
	Width     = 4
	Group     = 16
	GroupMask = 0xF
)

type Map struct {
	Vs [][]int
	Ks [][]uint64
}

func New() *Map {
	vs := make([][]int, Group)
	ks := make([][]uint64, Group)
	for i := 0; i < Group; i++ {
		vs[i] = make([]int, 0, 16)
		ks[i] = make([]uint64, 0, 16)
	}
	return &Map{Ks: ks, Vs: vs}
}

func (m *Map) Get(k uint64) (int, bool) {
	slot := k & GroupMask
	j := len(m.Ks[slot]) / Width
	for i := j * Width; i < len(m.Ks[slot]); i++ {
		if m.Ks[slot][i] == k {
			return m.Vs[slot][i], true
		}
	}
	return -1, false
}

func (m *Map) Set(k uint64, v int) {
	slot := k & GroupMask
	m.Vs[slot] = append(m.Vs[slot], v)
	m.Ks[slot] = append(m.Ks[slot], k)
	return
}

func (m *Map) Reset() {
	for i := 0; i < Group; i++ {
		m.Ks[i] = m.Ks[i][:0]
		m.Vs[i] = m.Vs[i][:0]
	}
}
