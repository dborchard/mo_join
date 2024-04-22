package hashtable

import "unsafe"

type Int64HashMapCell struct {
	Key    uint64
	Mapped uint64
}
type Int64HashMap struct {
	cellCntBits uint8
	cellCnt     uint64
	cellCntMask uint64
	elemCnt     uint64
	maxElemCnt  uint64
	cells       []Int64HashMapCell
	//confCnt     uint64
}

func (ht *Int64HashMap) Init() {
	ht.cellCntBits = kInitialCellCntBits
	ht.cellCnt = kInitialCellCnt
	ht.cellCntMask = kInitialCellCnt - 1
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialCellCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.cells = make([]Int64HashMapCell, kInitialCellCnt)
}

func (ht *Int64HashMap) InsertBatch(n int, hashes []uint64, keysPtr unsafe.Pointer, values []uint64) {

	keys := unsafe.Slice((*uint64)(keysPtr), n)

	for i, key := range keys {
		cell := ht.findCell(hashes[i], key)
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.Key = key
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *Int64HashMap) findCell(hash uint64, key uint64) *Int64HashMapCell {
	for idx := hash & ht.cellCntMask; true; idx = (idx + 1) & ht.cellCntMask {
		cell := &ht.cells[idx]
		if cell.Key == key || cell.Mapped == 0 {
			return cell
		}
		//ht.confCnt++
	}

	return nil
}
