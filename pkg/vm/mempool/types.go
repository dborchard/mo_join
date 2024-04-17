package mempool

var OneCount = []byte{1, 0, 0, 0, 0, 0, 0, 0}

const (
	CountSize  = 8
	PageSize   = 64
	PageOffset = 6

	// HeaderSize is used for storing the Header Information on mpool allocated
	// byte[].
	HeaderSize = 8
)
