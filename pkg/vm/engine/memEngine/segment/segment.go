package segment

import (
	"mo_join/pkg/vm/engine/memEngine/kv"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/metadata"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/container/vector"
	"mo_join/pkg/z/encoding"
)

type Segment struct {
	id   string
	db   *kv.KV
	proc *process.Process
	mp   map[string]metadata.Attribute
}

func (s *Segment) ID() string {
	return s.id
}

func New(id string, db *kv.KV, proc *process.Process, attrs []metadata.Attribute) *Segment {
	mp := make(map[string]metadata.Attribute)
	for _, attr := range attrs {
		mp[attr.Name] = attr
	}
	return &Segment{id, db, proc, mp}
}

func (s *Segment) Read(cs []uint64, attrs []string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(true, attrs)
	for i, attr := range attrs {
		md := s.mp[attr]
		vec, err := s.read(s.id+"."+attr, md.Type, proc)
		if err != nil {
			for j := 0; j < i; j++ {
				copy(bat.Vecs[j].Data, mempool.OneCount)
				bat.Vecs[j].Free(s.proc)
			}
			return nil, err
		}
		//TODO: later replace this code
		copy(vec.Data, encoding.EncodeUint64(cs[i]))
		bat.Vecs[i] = vec
	}
	return bat, nil
}

func (s *Segment) read(id string, typ types.Type, proc *process.Process) (*vector.Vector, error) {
	data, err := s.db.Get(id, proc)
	if err != nil {
		return nil, err
	}

	vec := vector.New(typ)
	if err := vec.Read(data); err != nil {
		proc.Free(data)
		return nil, err
	}
	return vec, nil
}
