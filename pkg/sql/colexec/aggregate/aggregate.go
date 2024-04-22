// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregate

import (
	"fmt"
	"mo_join/pkg/z/container/ring"
	"mo_join/pkg/z/container/ring/max"
	"mo_join/pkg/z/container/types"
)

func New(op int, dist bool, typ types.Type) (ring.Ring, error) {
	switch op {
	case Max:
		return NewMax(typ)
	default:
		panic("not implemented")
	}
	return nil, nil
}

func NewMax(typ types.Type) (ring.Ring, error) {
	switch typ.Oid {
	case types.T_int8:
		return max.NewInt8(typ), nil
	}
	return nil, fmt.Errorf("'%v' not support Max", typ)
}
