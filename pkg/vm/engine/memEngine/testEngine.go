package memEngine

import (
	"mo_join/pkg/vm/engine"
	"mo_join/pkg/vm/engine/memEngine/kv"
)

func NewTestEngine() engine.Engine {
	e := New(kv.New())
	CreateR(e)
	CreateS(e)
	return e
}
