package mempool

const (
	HeaderSize = 8
)

type Mempool struct {
	maxSize  int
	currSize int
}

func (m *Mempool) Size() int64 {
	return int64(m.currSize)
}

func (m *Mempool) Alloc(size int) []byte {
	m.currSize += size
	if m.currSize > m.maxSize {
		panic("out of memory")
	}
	return make([]byte, size)
}

func (m *Mempool) Free(data []byte) bool {
	m.currSize -= cap(data)
	return true
}
