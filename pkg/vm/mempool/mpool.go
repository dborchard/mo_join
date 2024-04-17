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

func (m *Mempool) Alloc(size int) []byte {
	m.currSize += size
	size = ((size + PageSize - 1 + CountSize) >> PageOffset) << PageOffset
	if size > m.maxSize {
		panic("size too large")
	}
	data := make([]byte, size)
	copy(data, OneCount)
	return data
}

func (m *Mempool) Free(data []byte) bool {
	m.currSize -= cap(data)
	return true
}

func Alloc(m *Mempool, size int) (ret []byte) {
	return make([]byte, size)
}
