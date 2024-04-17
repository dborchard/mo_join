package vector

import (
	"fmt"
	"log"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/process"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/encoding"

	"testing"
)

func TestVector(t *testing.T) {
	oldVec := New(types.Type{Oid: types.T(types.TVarchar), Size: 24})
	newVec := New(types.Type{Oid: types.T(types.TVarchar), Size: 24})
	{
		vs := make([][]byte, 10)
		for i := 0; i < 10; i++ {
			vs[i] = []byte(fmt.Sprintf("%v", i*i))
		}
		vs[9] = []byte("abcd")
		if err := oldVec.Append(vs); err != nil {
			log.Fatal(err)
		}
		oldVec.Data = encoding.EncodeInt64(1)
	}

	{
		fmt.Printf("v: %v\n", oldVec)
		fmt.Printf("w: %v\n", newVec)
	}

	proc := process.New(mempool.New(1<<32, 8))
	for i := 0; i < 5; i++ {
		if err := newVec.UnionOne(oldVec, int64(i), proc); err != nil {
			log.Fatal(err)
		}
	}
	{
		fmt.Printf("v: %v\n", oldVec)
		fmt.Printf("w: %v\n", newVec)
	}
	{
		if err := newVec.Copy(oldVec, 1, 9, proc); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("w[0] = v[6]: %v\n", newVec)
	}
	newVec.Free(proc)
	fmt.Printf("guest: %v\n", proc.Size())
}
