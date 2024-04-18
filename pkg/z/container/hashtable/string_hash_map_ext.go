package hashtable

func (ht *StringHashMap) FindStringBatch(states [][3]uint64, keys [][]byte, values []uint64) {

	for i := range keys {
		cell := ht.findCell(&states[i])
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) InsertStringBatchWithRing(zValues []int64, states [][3]uint64, keys [][]byte, values []uint64) {

	for i := range keys {
		if zValues[i] == 0 {
			continue
		}

		cell := ht.findCell(&states[i])
		if cell.Mapped == 0 {
			ht.elemCnt++
			cell.HashState = states[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *StringHashMap) findCell(state *[3]uint64) *StringHashMapCell {
	mask := ht.cellCnt - 1
	for idx := state[0] & mask; true; idx = (idx + 1) & mask {
		cell := &ht.cells[idx]
		if cell.Mapped == 0 || cell.HashState == *state {
			return cell
		}
		//ht.confCnt++
	}

	return nil
}
