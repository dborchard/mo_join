package engine

import (
	"mo_join/pkg/vm/metadata"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
)

type Engine interface {
	Relations() []Relation
	Relation(string) (Relation, error)

	Delete(string) error
	Create(string, []metadata.Attribute) error
}

type Relation interface {
	ID() string

	Segments() []string
	Attribute() []metadata.Attribute

	Segment(string, *process.Process) Segment

	Write(*batch.Batch) error

	AddAttribute(metadata.Attribute) error
	DelAttribute(metadata.Attribute) error
}

type Segment interface {
	ID() string
	Read([]uint64, []string, *process.Process) (*batch.Batch, error) // read only arguments
}
