package hashmap

import "mojoins/pkg/z/pb/plan"

// JoinMap is used for join
type JoinMap struct {
	cnt       *int64
	dupCnt    *int64
	multiSels [][]int32
	// push-down filter expression, possibly a bloomfilter
	expr    *plan.Expr
	shm     *StrHashMap
	ihm     *IntHashMap
	hasNull bool

	isDup            bool
	runtimeFilter_In bool
}

func (jm *JoinMap) Free() {

}

func (jm *JoinMap) Size() int64 {
	return 9
}

func (jm *JoinMap) Sels() [][]int32 {
	return jm.multiSels
}

func (jm *JoinMap) NewIterator() Iterator {
	if jm.shm == nil {
		return &intHashMapIterator{
			mp:      jm.ihm,
			m:       jm.ihm.m,
			ibucket: jm.ihm.ibucket,
			nbucket: jm.ihm.nbucket,
		}
	} else {
		return &strHashmapIterator{
			mp:      jm.shm,
			m:       jm.shm.m,
			ibucket: jm.shm.ibucket,
			nbucket: jm.shm.nbucket,
		}
	}
}
