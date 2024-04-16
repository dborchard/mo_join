package hashmap

import (
	"mojoins/pkg/common/mpool"
	"mojoins/pkg/container/hashtable"
	"mojoins/pkg/z/container/vector"
)

const (
	UnitLimit             = 256
	HashMapSizeThreshHold = UnitLimit * 128
	HashMapSizeEstimate   = UnitLimit * 32
)

// Iterator allows users to do insert or find operations on hash tables in bulk.
type Iterator interface {
	// Insert vecs[start, start+count) into hashmap
	// vs  : the number of rows corresponding to each value in the hash table (start with 1)
	// zvs : if zvs[i] is 0 indicates the presence null, 1 indicates the absence of a null.
	Insert(start, count int, vecs []*vector.Vector) (vs []uint64, zvs []int64, err error)

	// Find vecs[start, start+count) in hashmap
	// vs  : the number of rows corresponding to each value in the hash table (start with 1, and 0 means not found.)
	// zvs : if zvs[i] is 0 indicates the presence null, 1 indicates the absence of a null.
	Find(start, count int, vecs []*vector.Vector, inBuckets []uint8) (vs []uint64, zvs []int64)
}

type strHashmapIterator struct {
	m                *mpool.MPool
	mp               *StrHashMap
	ibucket, nbucket uint64
}

type intHashMapIterator struct {
	ibucket, nbucket uint64
	m                *mpool.MPool
	mp               *IntHashMap
}

// StrHashMap key is []byte, value is an uint64 value (starting from 1)
//
//	each time a new key is inserted, the hashtable returns a last-value+1 or, if the old key is inserted, the value corresponding to that key
type StrHashMap struct {
	hasNull bool
	rows    uint64
	keys    [][]byte
	values  []uint64
	// zValues, 0 indicates the presence null, 1 indicates the absence of a null
	zValues       []int64
	strHashStates [][3]uint64
	ibucket       uint64
	nbucket       uint64

	m       *mpool.MPool
	hashMap *hashtable.StringHashMap
}

// IntHashMap key is int64, value is an uint64 (start from 1)
// before you use the IntHashMap, the user should make sure that
// sum of vectors' length equal to 8
type IntHashMap struct {
	hasNull bool

	rows    uint64
	keys    []uint64
	keyOffs []uint32
	values  []uint64
	zValues []int64
	hashes  []uint64
	ibucket uint64
	nbucket uint64

	m       *mpool.MPool
	hashMap *hashtable.Int64HashMap
}

var (
	OneUInt8s  []uint8
	OneInt64s  []int64
	zeroUint64 []uint64
	zeroUint32 []uint32
)

// HashMap is the encapsulated hash table interface exposed to the outside
type HashMap interface {
	// HasNull returns whether the hash map considers the null values.
	HasNull() bool
	// Free method frees the hash map.
	Free()
	// AddGroup adds 1 to the row count of hash map.
	AddGroup()
	// AddGroups adds N to the row count of hash map.
	AddGroups(uint64)
	// GroupCount returns the hash map's row count.
	GroupCount() uint64
	// Size returns the hash map's size
	Size() int64
}
