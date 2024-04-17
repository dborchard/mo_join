package mempool

type Mempool struct {
	maxSize  int
	currSize int
}

func New(maxSize, factor int) *Mempool {
	m := &Mempool{
		maxSize: maxSize,
	}
	return m
}

func (m *Mempool) Size() int64 {
	return int64(m.currSize)
}

func Alloc(m *Mempool, size int) (ret []byte) {
	return make([]byte, size)
}
