package mempool

func New(maxSize, factor int) *Mempool {
	m := &Mempool{
		maxSize: maxSize,
	}
	return m
}

var OneCount = []byte{1, 0, 0, 0, 0, 0, 0, 0}
