package plan

type TableDef struct {
	Pkey *PrimaryKeyDef
}

type PrimaryKeyDef struct {
	CompPkeyCol *ColDef
}

type ColDef struct {
	Seqnum uint32
}

type ObjectRef struct {
	ObjName string
}

type RuntimeFilterSpec struct {
}
