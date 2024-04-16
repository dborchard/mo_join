package memEngine

import (
	"mo_join/pkg/vm/engine"
	"mo_join/pkg/vm/engine/memEngine/kv"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/metadata"
	"mo_join/pkg/vm/process"
)

func New(db *kv.KV) *memEngine {
	return &memEngine{db, process.New(mempool.New(1<<32, 16))}
}

func (m *memEngine) Relations() []engine.Relation {
	//TODO implement me
	panic("implement me")
}

func (m *memEngine) Relation(s string) (engine.Relation, error) {
	//TODO implement me
	panic("implement me")
}

func (m *memEngine) Delete(s string) error {
	//TODO implement me
	panic("implement me")
}

func (m *memEngine) Create(s string, attributes []metadata.Attribute) error {
	//TODO implement me
	panic("implement me")
}
