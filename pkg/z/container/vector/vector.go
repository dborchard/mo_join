package vector

import (
	"bytes"
	"errors"
	"fmt"
	"mo_join/pkg/vm/mempool"
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
			v.Col = encoding.DecodeFloat64Slice(data)
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeFloat64Slice(data[size:])
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
		Col.Lengths = encoding.DecodeUint32Slice(data[:4*cnt])
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
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeFloat64Slice(v.Col.([]float64)))
		return buf.Bytes(), nil

	case types.T_varchar:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		Col := v.Col.(*types.Bytes)
		cnt := int32(len(Col.Offsets))
		buf.Write(encoding.EncodeInt32(cnt))
		if cnt == 0 {
			return buf.Bytes(), nil
		}
		buf.Write(encoding.EncodeUint32Slice(Col.Lengths))
		buf.Write(Col.Data)
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupport encoding type %s", v.Typ.Oid)
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
				v.Col = encoding.DecodeFloat64Slice(data[mempool.HeaderSize : mempool.HeaderSize+len(oldData)*8])
				v.Data = data
				oldData = v.Col.([]float64)
			}
		}
		v.Col = append(oldData, newData[sel])

	case types.T_varchar:
		newData := w.Col.(*types.Bytes)
		from := newData.Data[newData.Offsets[sel] : newData.Offsets[sel]+newData.Lengths[sel]]
		oldData := v.Col.(*types.Bytes)
		{
			if v.Data == nil || cap(v.Data[mempool.HeaderSize:]) < len(oldData.Data)+len(from) {
				data, err := proc.Alloc(int64(len(oldData.Data) + len(from)))
				if err != nil {
					return err
				}
				if v.Data != nil {
					copy(data, v.Data)
					proc.Free(v.Data)
				} else {
					copy(data[:mempool.HeaderSize], w.Data[:mempool.HeaderSize])
				}
				data = data[:mempool.HeaderSize+len(oldData.Data)]
				v.Data = data
				oldData.Data = data[mempool.HeaderSize:]
			}
		}
		oldData.Lengths = append(oldData.Lengths, uint32(len(from)))
		{
			n := len(oldData.Offsets)
			if n > 0 {
				oldData.Offsets = append(oldData.Offsets, oldData.Offsets[n-1]+oldData.Lengths[n-1])
			} else {
				oldData.Offsets = append(oldData.Offsets, 0)
			}
		}
		oldData.Data = append(oldData.Data, from...)
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