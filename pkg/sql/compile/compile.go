package compile

import (
	"fmt"
	"mo_join/pkg/sql/colexec/connector"
	"mo_join/pkg/sql/plan"
	"mo_join/pkg/vm"
	"mo_join/pkg/vm/mheap"
	"mo_join/pkg/vm/overload"
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
			chp.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(children))
			for j := range children {
				children[j].Instructions = append(children[j].Instructions, vm.Instruction{
					Op: overload.Connector,
					Arg: &connector.Argument{
						Mmu: chp.Proc.Mp.Gm,
						Reg: chp.Proc.Reg.MergeReceivers[j],
					},
				})
			}
			chp.Instructions = append(chp.Instructions, vm.Instruction{
				Op:  overload.Merge,
				Arg: &merge.Argument{},
			})
		}
		rs[i] = &Scope{
			Magic:     Remote,
			PreScopes: []*Scope{ss[i], chp},
		}
		rs[i].Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, 2)
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: overload.Connector,
			Arg: &connector.Argument{
				Mmu: rs[i].Proc.Mp.Gm,
				Reg: rs[i].Proc.Reg.MergeReceivers[0],
			},
		})
		chp.Instructions = append(chp.Instructions, vm.Instruction{
			Op: overload.Connector,
			Arg: &connector.Argument{
				Mmu: rs[i].Proc.Mp.Gm,
				Reg: rs[i].Proc.Reg.MergeReceivers[1],
			},
		})
	}
	switch joinTyp {
	case plan.Node_INNER:
		if len(n.OnList) == 0 {
			for i := range rs {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  overload.Product,
					Arg: constructProduct(n, c.proc),
				})
			}
		} else {
			for i := range rs {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  overload.Join,
					Arg: constructJoin(n, c.proc),
				})
			}
		}
	case plan.Node_SEMI:
		for i := range rs {
			rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
				Op:  overload.Semi,
				Arg: constructSemi(n, c.proc),
			})
		}
	case plan.Node_OUTER:
		for i := range rs {
			rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
				Op:  overload.Left,
				Arg: constructLeft(n, c.proc),
			})
		}
	case plan.Node_ANTI:
		for i := range rs {
			rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
				Op:  overload.Complement,
				Arg: constructComplement(n, c.proc),
			})
		}
	default:
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("join typ '%v' not support now", n.JoinType)))
	}
	return rs
}
