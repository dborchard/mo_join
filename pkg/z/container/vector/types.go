package vector

import (
	"bytes"
	"fmt"
	"mo_join/pkg/encoding"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/nulls"
	"mo_join/pkg/z/container/types"
)

type Vector struct {
	Or   bool   // true: origin
	Data []byte // raw data
	Typ  types.Type
	Col  interface{}
	Nsp  *nulls.Nulls
}

func (v *Vector) Clean(p *process.Process) {
	if v.Data != nil {
		if p.Free(v.Data) {
			v.Data = nil
		}
	}
}

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
	data = data[mempool.CountSize:]
	typ := encoding.DecodeType(data[:encoding.TypeSize])
	data = data[encoding.TypeSize:]
	v.Typ = typ
	v.Or = true
	switch typ.Oid {

	case types.T_float64:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeFloat64Slice(data[4:])
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
