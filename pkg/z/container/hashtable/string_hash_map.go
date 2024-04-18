package hashtable

var StrKeyPadding [16]byte

type StringHashMap struct {
	cellCntBits uint8
	cellCnt     uint64
	elemCnt     uint64
	maxElemCnt  uint64
	cells       []StringHashMapCell
	//confCnt     uint64
}

type StringHashMapCell struct {
	HashState [3]uint64
	Mapped    uint64
}

func (ht *StringHashMap) Init() {
	ht.cellCntBits = kInitialCellCntBits
	ht.cellCnt = kInitialCellCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.cells = make([]StringHashMapCell, kInitialCellCnt)
}
