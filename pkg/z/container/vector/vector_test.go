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
	v := New(types.Type{Oid: types.T(types.T_varchar), Size: 24})
	w := New(types.Type{Oid: types.T(types.T_varchar), Size: 24})
	{
		vs := make([][]byte, 10)
		for i := 0; i < 10; i++ {
			vs[i] = []byte(fmt.Sprintf("%v", i*i))
		}
		vs[9] = []byte("abcd")
		if err := v.Append(vs); err != nil {
			log.Fatal(err)
		}
		v.Data = encoding.EncodeInt64(1)
	}
	proc := process.New(mempool.New(1<<32, 8))
	for i := 0; i < 5; i++ {
		if err := w.UnionOne(v, int64(i), proc); err != nil {
			log.Fatal(err)
		}
	}
	{
		fmt.Printf("v: %v\n", v)
		fmt.Printf("w: %v\n", w)
	}
	{
		if err := w.Copy(v, 1, 9, proc); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("w[0] = v[6]: %v\n", w)
	}
	w.Free(proc)
	fmt.Printf("guest: %v\n", proc.Size())
}
