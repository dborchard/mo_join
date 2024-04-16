package batch

import (
	"bytes"
	"fmt"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/vector"
)

type Batch struct {
	Ro    bool
	Sels  []int64
	Attrs []string
	Vecs  []*vector.Vector
}

func New(ro bool, attrs []string) *Batch {
	return &Batch{
		Ro:    ro,
		Attrs: attrs,
		Vecs:  make([]*vector.Vector, len(attrs)),
	}
}

func (bat *Batch) Clean(proc *process.Process) {
	bat.Sels = nil
	for _, vec := range bat.Vecs {
		vec.Free(proc)
	}
}

func (bat *Batch) Cow() {
	attrs := make([]string, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrs[i] = attr
	}
	bat.Ro = false
	bat.Attrs = attrs
}

func (bat *Batch) Reorder(attrs []string) {
	if bat.Ro {
		bat.Cow()
	}

	attrIndex := make(map[string]int, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrIndex[attr] = i
	}

	newVecs := make([]*vector.Vector, len(attrs))
	newAttrs := make([]string, len(attrs))

	for i, name := range attrs {
		if j, ok := attrIndex[name]; ok {
			newVecs[i] = bat.Vecs[j]
			newAttrs[i] = bat.Attrs[j]
		}
	}

	bat.Vecs = newVecs
	bat.Attrs = newAttrs
}

func (bat *Batch) Prefetch(attrs []string, vecs []*vector.Vector, proc *process.Process) error {
	var err error

	for i, attr := range attrs {
		if vecs[i], err = bat.GetVector(attr, proc); err != nil {
			return err
		}
	}
	return nil
}

func (bat *Batch) GetVector(name string, proc *process.Process) (*vector.Vector, error) {
	for i, attr := range bat.Attrs {
		if attr != name {
			continue
		}

		data := bat.Vecs[i].Data
		if err := bat.Vecs[i].Read(data); err != nil {
			return nil, err
		}
		return bat.Vecs[i], nil
	}
	return nil, fmt.Errorf("attribute '%s' not exist", name)
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	if len(bat.Sels) > 0 {
		fmt.Printf("%v\n", bat.Sels)
	}
	for i, attr := range bat.Attrs {
		buf.WriteString(fmt.Sprintf("%s\n", attr))
		buf.WriteString(fmt.Sprintf("\t%s\n", bat.Vecs[i]))
	}
	return buf.String()
}
