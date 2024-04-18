package engine

import "mo_join/pkg/z/container/batch"

type Snapshot []byte

type Relation interface {
	//Statistics

	Close(Snapshot)

	ID(Snapshot) string

	//Nodes(Snapshot) Nodes
	//
	//TableDefs(Snapshot) []TableDef
	//
	//GetPrimaryKeys(Snapshot) []*Attribute
	//
	//GetHideKey(Snapshot) *Attribute
	//// true: primary key, false: hide key
	//GetPriKeyOrHideKey(Snapshot) ([]Attribute, bool)
	//
	//Write(uint64, *batch.Batch, Snapshot) error
	//
	//Delete(uint64, *vector.Vector, string, Snapshot) error
	//
	//AddTableDef(uint64, TableDef, Snapshot) error
	//DelTableDef(uint64, TableDef, Snapshot) error
	//
	//// first argument is the number of reader, second argument is the filter extend,  third parameter is the payload required by the engine
	//NewReader(int, extend.Extend, []byte, Snapshot) []Reader
}

type Database interface {
	Relations(Snapshot) []string
	Relation(string, Snapshot) (Relation, error)

	Delete(uint64, string, Snapshot) error
	Create(uint64, string, []TableDef, Snapshot) error // Create Table - (name, table define)
}

type TableDef interface {
	tableDef()
}

type Engine interface {
	Delete(uint64, string, Snapshot) error
	Create(uint64, string, int, Snapshot) error // Create Database - (name, engine type)

	Databases(Snapshot) []string
	Database(string, Snapshot) (Database, error)

	//Node(string, Snapshot) *NodeInfo
}
type Reader interface {
	Read([]uint64, []string) (*batch.Batch, error)
}

type Node struct {
	Id   string `json:"id"`
	Addr string `json:"address"`
	Data []byte `json:"payload"`
}
