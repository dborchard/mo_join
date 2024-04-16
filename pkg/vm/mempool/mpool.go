package mempool

func New(maxSize, factor int) *Mempool {
	m := &Mempool{
		maxSize: maxSize,
	}
	return m
}
