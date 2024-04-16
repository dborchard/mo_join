package unittest

import (
	"log"
	"mo_join/pkg/vm/engine"
	"mo_join/pkg/vm/process"
)

func segments(name string, proc *process.Process) []engine.Segment {
	e := memEngine.NewTestEngine()
	r, err := e.Relation(name)
	if err != nil {
		log.Fatal(err)
	}
	ids := r.Segments()
	segs := make([]engine.Segment, len(ids))
	for i, id := range ids {
		segs[i] = r.Segment(id, proc)
	}
	return segs
}
