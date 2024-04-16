package unittest

import (
	"fmt"
	"mo_join/pkg/sql/colexec/bag/inner"
	"mo_join/pkg/sql/colexec/transfer"
	"mo_join/pkg/vm"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/pipeline"
	"mo_join/pkg/vm/process"

	"sync"
	"testing"
)

func TestBagInnerJoin(t *testing.T) {
	var wg sync.WaitGroup
	var ins vm.Instructions

	proc := process.New(mempool.New(1<<32, 8))
	{
		proc.Refer = make(map[string]uint64)

		proc.Reg.WaitRegisters = make([]*process.WaitRegister, 2)
		for i := 0; i < 2; i++ {
			proc.Reg.WaitRegisters[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}),
			}
		}
	}
	// R table
	{
		var rInstructions vm.Instructions

		rProcess := process.New(mempool.New(1<<32, 8))
		{
			rProcess.Refer = make(map[string]uint64)
		}
		rInstructions = append(rInstructions, vm.Instruction{Op: vm.Transfer, Arg: &transfer.Argument{Reg: proc.Reg.WaitRegisters[0]}})
		rp := pipeline.New([]uint64{1, 1, 1}, []string{"orderId", "uid", "price"}, rInstructions)
		wg.Add(1)
		go func() {
			fmt.Printf("R: %s\n", rp)
			rp.Run(segments("R", rProcess), rProcess)
			fmt.Printf("R - guest: %v\n", rProcess.Size())
			wg.Done()
		}()
	}
	{
		var sins vm.Instructions

		sproc := process.New(mempool.New(1<<32, 8))
		{
			sproc.Refer = make(map[string]uint64)
		}
		sins = append(sins, vm.Instruction{Op: vm.Transfer, Arg: &transfer.Argument{Reg: proc.Reg.WaitRegisters[1]}})
		sp := pipeline.New([]uint64{1, 1, 1}, []string{"uid", "price", "orderId"}, sins)
		wg.Add(1)
		go func() {
			fmt.Printf("S: %s\n", sp)
			sp.Run(segments("S", sproc)[:1], sproc)
			fmt.Printf("S - guest: %v\n", sproc.Size())
			wg.Done()
		}()
	}
	{
		ins = append(ins, vm.Instruction{Op: vm.BagInnerJoin, Arg: &inner.Argument{R: "R", S: "S", Attrs: []string{"uid"}}})
		ins = append(ins, vm.Instruction{Op: vm.Output})
	}
	p := pipeline.NewMerge(ins)
	fmt.Printf("%s\n", p)
	p.RunMerge(proc)
	fmt.Printf("guest: %v\n", proc.Size())
	wg.Wait()
	fmt.Printf("************\n")
}
