// Copyright 2022 Matrix Origin
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

package preinsertsecondaryindex

import (
	"context"
	"mo_join/pkg/vm"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/pb/plan"
	"mo_join/pkg/z/util"
)

type Argument struct {
	Ctx          context.Context
	PreInsertCtx *plan.PreInsertUkCtx

	packer util.PackerList

	buf *batch.Batch

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func (arg *Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return &Argument{}
}

func (arg *Argument) Release() {

}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.buf != nil {
		arg.buf.Clean(proc.Mp())
	}
	arg.packer.Free()
}
