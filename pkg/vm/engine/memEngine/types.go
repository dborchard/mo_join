package memEngine

import (
	"mo_join/pkg/vm/engine/memEngine/kv"
	"mo_join/pkg/vm/engine/memEngine/meta"
	"mo_join/pkg/vm/process"
)

// standalone memory engine
type memEngine struct {
	db   *kv.KV
	proc *process.Process
}

type relation struct {
	id string
	db *kv.KV
	md meta.Metadata
}
