package mpool

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// Stats
type MPoolStats struct {
	NumAlloc      atomic.Int64 // number of allocations
	NumFree       atomic.Int64 // number of frees
	NumAllocBytes atomic.Int64 // number of bytes allocated
	NumFreeBytes  atomic.Int64 // number of bytes freed
	NumCurrBytes  atomic.Int64 // current number of bytes
	HighWaterMark atomic.Int64 // high water mark
}

const (
	NumFixedPool = 5
	kMemHdrSz    = 16
	kStripeSize  = 128
	B            = 1
	KB           = 1024
	MB           = 1024 * KB
	GB           = 1024 * MB
	TB           = 1024 * GB
	PB           = 1024 * TB
)

// pool for fixed elements.  Note that we preconfigure the pool size.
// We should consider implement some kind of growing logic.
type fixedPool struct {
	m      sync.Mutex
	noLock bool
	fpIdx  int8
	poolId int64
	eleSz  int32
	// holds buffers allocated, it is not really used in alloc/free
	// but hold here for bookkeeping.
	buf   [][]byte
	flist unsafe.Pointer
}
type detailInfo struct {
	cnt, bytes int64
}

type mpoolDetails struct {
	mu    sync.Mutex
	alloc map[string]detailInfo
	free  map[string]detailInfo
}

// The memory pool.
type MPool struct {
	id         int64      // mpool generated, used to look up the MPool
	tag        string     // user supplied, for debug/inspect
	cap        int64      // pool capacity
	stats      MPoolStats // stats
	noFixed    bool
	noLock     bool
	available  int32 // 0: available, 1: unavailable
	inUseCount int32 // number of in use call
	pools      [NumFixedPool]fixedPool
	details    *mpoolDetails

	// To remove: this thing is highly unlikely to be of any good use.
	sels *sync.Pool
}
