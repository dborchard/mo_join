package vector

import (
	"bytes"
	"errors"
	"fmt"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/mheap"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/nulls"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/encoding"
	"reflect"
	"unsafe"
)

func New(typ types.Type) *Vector {
	switch typ.Oid {

	case types.T_float64:
		return &Vector{
			Typ: typ,
			Col: []float64{},
			Nsp: &nulls.Nulls{},
		}

	case types.T_varchar:
		return &Vector{
			Typ: typ,
			Col: &types.Bytes{},
			Nsp: &nulls.Nulls{},
		}
	}
	return nil
}

func (v *Vector) Append(arg interface{}) error {
	switch v.Typ.Oid {

	case types.T_float64:
		v.Col = append(v.Col.([]float64), arg.([]float64)...)
	case types.T_varchar:
		return v.Col.(*types.Bytes).Append(arg.([][]byte))
	}
	return nil
}

func (v *Vector) Free(p *process.Process) {
	if v.Data != nil {
		if p.Free(v.Data) {
			v.Data = nil
		}
	}
}

func (v *Vector) Read(data []byte) error {
	v.Data = data
	data = data[mempool.HeaderSize:]

	typ := encoding.DecodeType(data[:encoding.TypeSize])
	v.Typ = typ
	v.Or = true

	data = data[encoding.TypeSize:]

	switch typ.Oid {

	case types.T_float64:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			data = data[4:]
			v.Col = encoding.DecodeSlice[float64](data)
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeSlice[float64](data[size:])
		}

	case types.T_varchar:
		Col := v.Col.(*types.Bytes)
		Col.Reset()

		size := encoding.DecodeUint32(data)
		data = data[4:]

		if size > 0 {
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			data = data[size:]
		}

		cnt := encoding.DecodeInt32(data)
		if cnt == 0 {
			break
		}

		data = data[4:]
		Col.Offsets = make([]uint32, cnt)
		Col.Lengths = encoding.DecodeSlice[uint32](data[:4*cnt])
		Col.Data = data[4*cnt:]
		{
			o := uint32(0)
			for i, n := range Col.Lengths {
				Col.Offsets[i] = o
				o += n
			}
		}
	}
	return nil
}

func (v *Vector) Show() ([]byte, error) {
	var buf bytes.Buffer

	switch v.Typ.Oid {

	case types.T_float64:
		buf.Write(encoding.EncodeType(&v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		uint32Len := uint32(len(nb))
		buf.Write(encoding.EncodeUint32(&uint32Len))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeSlice[float64](v.Col.([]float64)))
		return buf.Bytes(), nil

	case types.T_varchar:
		buf.Write(encoding.EncodeType(&v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		uint32Len := uint32(len(nb))
		buf.Write(encoding.EncodeUint32(&uint32Len))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		Col := v.Col.(*types.Bytes)
		cnt := int32(len(Col.Offsets))
		buf.Write(encoding.EncodeInt32(&cnt))
		if cnt == 0 {
			return buf.Bytes(), nil
		}
		buf.Write(encoding.EncodeSlice[uint32](Col.Lengths))
		buf.Write(Col.Data)
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupport encoding type %v", v.Typ.Oid)
	}
}

func (v *Vector) Length() int {
	switch v.Typ.Oid {
	case types.T_varchar:
		return len(v.Col.(*types.Bytes).Offsets)
	default:
		hp := *(*reflect.SliceHeader)((*(*emptyInterface)(unsafe.Pointer(&v.Col))).word)
		return hp.Len
	}
}

func (v *Vector) UnionOne(w *Vector, sel int64, proc *process.Process) error {
	if v.Or {
		return errors.New("unionone operation cannot be performed for origin vector")
	}
	switch v.Typ.Oid {

	case types.T_float64:
		newData := w.Col.([]float64)
		oldData := v.Col.([]float64)
		{
			if v.Data == nil || cap(v.Data[mempool.HeaderSize:]) < (len(newData)+1)*8 {
				data, err := proc.Alloc(int64(len(newData)+1) * 8)
				if err != nil {
					return err
				}
				if v.Data != nil {
					copy(data, v.Data)
					proc.Free(v.Data)
				} else {
					copy(data[:mempool.HeaderSize], w.Data[:mempool.HeaderSize])
				}
				v.Col = encoding.DecodeSlice[float64](data[mempool.HeaderSize : mempool.HeaderSize+len(oldData)*8])
				v.Data = data
				oldData = v.Col.([]float64)
			}
		}
		v.Col = append(oldData, newData[sel])

	case types.T_varchar:
		vs := w.Col.(*types.Bytes)
		from := vs.Data[vs.Offsets[sel] : vs.Offsets[sel]+vs.Lengths[sel]]
		col := v.Col.(*types.Bytes)
		{
			if v.Data == nil || cap(v.Data[mempool.CountSize:]) < len(col.Data)+len(from) {
				data, err := proc.Alloc(int64(len(col.Data) + len(from)))
				if err != nil {
					return err
				}
				if v.Data != nil {
					copy(data, v.Data)
					proc.Free(v.Data)
				} else {
					copy(data[:mempool.CountSize], w.Data[:mempool.CountSize])
				}
				data = data[:mempool.CountSize+len(col.Data)]
				v.Data = data
				col.Data = data[mempool.CountSize:]
			}
		}
		col.Lengths = append(col.Lengths, uint32(len(from)))
		{
			n := len(col.Offsets)
			if n > 0 {
				col.Offsets = append(col.Offsets, col.Offsets[n-1]+col.Lengths[n-1])
			} else {
				col.Offsets = append(col.Offsets, 0)
			}
		}
		col.Data = append(col.Data, from...)
	}
	if w.Nsp.Any() && w.Nsp.Contains(uint64(sel)) {
		v.Nsp.Add(uint64(v.Length()))
	}
	return nil
}

func (v *Vector) Copy(w *Vector, vi, wi int64, proc *process.Process) error {
	vs, ws := v.Col.(*types.Bytes), w.Col.(*types.Bytes)
	data := ws.Data[ws.Offsets[wi] : ws.Offsets[wi]+ws.Lengths[wi]]
	if vs.Lengths[vi] >= ws.Lengths[wi] {
		vs.Lengths[vi] = ws.Lengths[wi]
		copy(vs.Data[vs.Offsets[vi]:int(vs.Offsets[vi])+len(data)], data)
		return nil
	}
	diff := ws.Lengths[wi] - vs.Lengths[vi]
	buf, err := proc.Alloc(int64(len(vs.Data) + int(diff)))
	if err != nil {
		return err
	}
	copy(buf[:mempool.CountSize], v.Data[:mempool.CountSize])
	copy(buf[mempool.CountSize:], vs.Data[:vs.Offsets[vi]])
	copy(buf[mempool.CountSize+vs.Offsets[vi]:], data)
	o := vs.Offsets[vi] + vs.Lengths[vi]
	copy(buf[mempool.CountSize+o+diff:], vs.Data[o:])
	proc.Free(v.Data)
	v.Data = buf
	vs.Data = buf[mempool.CountSize : mempool.CountSize+len(vs.Data)+int(diff)]
	vs.Lengths[vi] = ws.Lengths[wi]
	for i, j := vi+1, int64(len(vs.Offsets)); i < j; i++ {
		vs.Offsets[i] += diff
	}
	return nil
}

func (v *Vector) String() string {
	switch v.Typ.Oid {

	case types.T_float64:
		col := v.Col.([]float64)
		if len(col) == 1 {
			if v.Nsp.Contains(0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}

	case types.T_varchar:
		col := v.Col.(*types.Bytes)
		if len(col.Offsets) == 1 {
			if v.Nsp.Contains(0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%s", col.Data[:col.Lengths[0]])
			}
		}

	}
	return fmt.Sprintf("%v-%s", v.Col, v.Nsp)
}

func Clean(v *Vector, m *mheap.Mheap) {
	if !v.Or && v.Data != nil {
		mheap.Free(m, v.Data)
		v.Data = nil
	}
}

func Length(v *Vector) int {
	switch v.Typ.Oid {
	case types.T_varchar:
		return len(v.Col.(*types.Bytes).Offsets)
	default:
		return reflect.ValueOf(v.Col).Len()
	}
}

func UnionBatch(v, w *Vector, offset int64, cnt int, flags []uint8, m *mheap.Mheap) error {
	if v.Or {
		return errors.New("UnionOne operation cannot be performed for origin vector")
	}

	oldLen := Length(v)

	switch v.Typ.Oid {

	case types.T_float64:
		col := w.Col.([]float64)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeFloat64Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]float64)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+cnt)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeFloat64Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_varchar:
		vs, ws := v.Col.(*types.Bytes), w.Col.(*types.Bytes)
		incSize := 0
		for i, flag := range flags {
			if flag > 0 {
				incSize += int(ws.Lengths[int(offset)+i])
			}
		}

		if len(v.Data) == 0 {
			newSize := 8
			for newSize < incSize {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize))
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			v.Data = data
			vs.Data = data[:0]
		} else if n := len(vs.Data); n+incSize > cap(vs.Data) {
			data, err := mheap.Grow(m, vs.Data, int64(n+incSize))
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			v.Data = data
			vs.Data = data[:n]
		}

		for i, flag := range flags {
			if flag > 0 {
				from := ws.Get(offset + int64(i))
				vs.Lengths = append(vs.Lengths, uint32(len(from)))
				vs.Offsets = append(vs.Offsets, uint32(len(vs.Data)))
				vs.Data = append(vs.Data, from...)
			}
		}
		v.Col = vs

	}

	for i, j := 0, uint64(oldLen); i < len(flags); i++ {
		if flags[i] > 0 {
			if nulls.Contains(w.Nsp, uint64(offset)+uint64(i)) {
				nulls.Add(v.Nsp, j)
			}
			j++
		}
	}
	return nil
}

func UnionOne(v, w *Vector, sel int64, m *mheap.Mheap) error {
	if v.Or {
		return errors.New("UnionOne operation cannot be performed for origin vector")
	}
	switch v.Typ.Oid {

	case types.T_float64:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeFloat64Slice(data)
			vs[0] = w.Col.([]float64)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]float64)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeFloat64Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]float64)[sel])
			v.Col = vs
		}

	case types.T_varchar:
		vs, ws := v.Col.(*types.Bytes), w.Col.(*types.Bytes)
		from := ws.Get(sel)
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, int64(len(from))*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			v.Data = data
			vs.Data = data[:0]
		} else if n := len(vs.Data); n+len(from) >= cap(vs.Data) {
			data, err := mheap.Grow(m, vs.Data, int64(n+len(from)))
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			v.Data = data
			n = len(vs.Offsets)
			vs.Data = data[:vs.Offsets[n-1]+vs.Lengths[n-1]]
		}
		vs.Lengths = append(vs.Lengths, uint32(len(from)))
		{
			n := len(vs.Offsets)
			if n > 0 {
				vs.Offsets = append(vs.Offsets, vs.Offsets[n-1]+vs.Lengths[n-1])
			} else {
				vs.Offsets = append(vs.Offsets, 0)
			}
		}
		vs.Data = append(vs.Data, from...)
		v.Col = vs

	case types.T_decimal64:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeDecimal64Slice(data)
			vs[0] = w.Col.([]types.Decimal64)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Decimal64)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDecimal64Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]types.Decimal64)[sel])
			v.Col = vs
		}
	case types.T_decimal128:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 16*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeDecimal128Slice(data)
			vs[0] = w.Col.([]types.Decimal128)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Decimal128)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*16], int64(n+1)*16)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDecimal128Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]types.Decimal128)[sel])
			v.Col = vs
		}
	}
	if nulls.Any(w.Nsp) && nulls.Contains(w.Nsp, uint64(sel)) {
		nulls.Add(v.Nsp, uint64(Length(v)-1))
	}
	return nil
}

func DecodeFixedCol[T any](v *Vector, sz int) []T {
	return encoding.DecodeFixedSlice[T](v.Data, sz)
}
