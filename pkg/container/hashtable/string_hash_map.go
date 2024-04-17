package hashtable

type StringHashMapCell struct {
	HashState [3]uint64
	Mapped    uint64
}

type StringHashMap struct {
	blockCellCnt    uint64
	blockMaxElemCnt uint64
	cellCntMask     uint64
	//confCnt     uint64

	cellCnt uint64
	elemCnt uint64
	rawData [][]byte
	cells   [][]StringHashMapCell
}
