package metadata

import "mo_join/pkg/z/container/types"

type Attribute struct {
	Alg  int        // compression algorithm
	Name string     // name of attribute
	Type types.Type // type of attribute
}
