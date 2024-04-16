package memEngine

import (
	"mo_join/pkg/vm/engine"
	"mo_join/pkg/vm/engine/memEngine/kv"
	"mo_join/pkg/vm/engine/memEngine/meta"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/metadata"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/encoding"
)

func New(db *kv.KV) *memEngine {
	return &memEngine{db, process.New(mempool.New(1<<32, 16))}
}
func (e *memEngine) Create(name string, attrs []metadata.Attribute) error {
	var md meta.Metadata

	md.Name = name
	md.Attrs = attrs
	data, err := encoding.Encode(md)
	if err != nil {
		return err
	}
	return e.db.Set(name, data)
}

func (e *memEngine) Relation(name string) (engine.Relation, error) {
	var md meta.Metadata

	data, err := e.db.Get(name, e.proc)
	if err != nil {
		return nil, err
	}
	defer e.proc.Free(data)
	if err := encoding.Decode(data[mempool.HeaderSize:], &md); err != nil {
		return nil, err
	}
	return &relation{name, e.db, md}, nil
}

func (e *memEngine) Delete(name string) error {
	return e.db.Del(name)
}
