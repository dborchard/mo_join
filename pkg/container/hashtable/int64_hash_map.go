package hashtable

type Int64HashMapCell struct {
	Key    uint64
	Mapped uint64
}

type Int64HashMap struct {
	blockCellCnt    uint64
	blockMaxElemCnt uint64
	cellCntMask     uint64

	cellCnt uint64
	elemCnt uint64
	rawData [][]byte
	cells   [][]Int64HashMapCell
}
