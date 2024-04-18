package compile

import (
	"mo_join/pkg/sql/colexec/connector"
	"mo_join/pkg/sql/colexec/merge"
	"mo_join/pkg/sql/plan"
	"mo_join/pkg/vm"
	"mo_join/pkg/vm/mheap"
	"mo_join/pkg/vm/process"
)

func (c *Compile) compileJoin(n *plan.Node, ss []*Scope, children []*Scope, joinTyp plan.Node_JoinFlag) []*Scope {
	rs := make([]*Scope, len(ss))
	for i := range ss {
		chp := &Scope{
			PreScopes:   children,
			Magic:       Merge,
			DispatchAll: true,
		}
		{ // build merge scope for children
			chp.Proc = process.NewFromProc(mheap.New(), c.proc, len(children))
			for j := range children {
				children[j].Instructions = append(children[j].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Reg: chp.Proc.Reg.MergeReceivers[j],
					},
				})
			}
			chp.Instructions = append(chp.Instructions, vm.Instruction{
				Op:  vm.Merge,
				Arg: &merge.Argument{},
			})
		}
		rs[i] = &Scope{
			Magic:     Remote,
			PreScopes: []*Scope{ss[i], chp},
		}
		rs[i].Proc = process.NewFromProc(mheap.New(), c.proc, 2)
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: rs[i].Proc.Reg.MergeReceivers[0],
			},
		})
		chp.Instructions = append(chp.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: rs[i].Proc.Reg.MergeReceivers[1],
			},
		})
	}
	switch joinTyp {
	case plan.Node_INNER:
		if len(n.OnList) == 0 {

		} else {
			for i := range rs {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  vm.Join,
					Arg: constructJoin(n, c.proc),
				})
			}
		}

	default:
		panic("not supported")
	}
	return rs
}
