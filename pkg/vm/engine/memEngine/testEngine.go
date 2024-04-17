package memEngine

import (
	"fmt"
	"log"
	"mo_join/pkg/vm/engine"
	"mo_join/pkg/vm/engine/memEngine/kv"
	"mo_join/pkg/vm/metadata"
	"mo_join/pkg/z/container/batch"
	"mo_join/pkg/z/container/types"
	"mo_join/pkg/z/container/vector"
)

func NewTestEngine() engine.Engine {
	e := New(kv.New())
	CreateR(e)
	CreateS(e)
	return e
}

func CreateR(e engine.Engine) {
	{
		var attrs []metadata.Attribute

		{
			attrs = append(attrs, metadata.Attribute{
				Name: "orderId",
				Type: types.Type{Oid: types.T_varchar, Size: 24},
			})
			attrs = append(attrs, metadata.Attribute{
				Name: "uid",
				Type: types.Type{Oid: types.T_varchar, Size: 24},
			})
			attrs = append(attrs, metadata.Attribute{
				Name: "price",
				Type: types.Type{Oid: types.T_float64, Size: 8, Width: 8},
			})
		}
		if err := e.Create("R", attrs); err != nil {
			log.Fatal(err)
		}
	}
	r, err := e.Relation("R")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"orderId", "uid", "price"})
		{
			{
				vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i))
				}
				if err := vec.Append(vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[0] = vec
			}
			{
				vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i%4))
				}
				if err := vec.Append(vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[1] = vec
			}
			{
				vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vec.Append(vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[2] = vec
			}
		}
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
	{
		bat := batch.New(true, []string{"orderId", "uid", "price"})
		{
			vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i))
			}
			if err := vec.Append(vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i%4))
			}
			if err := vec.Append(vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.New(types.Type{Oid: types.T_float64, Size: 8, Width: 8})
			vs := make([]float64, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = float64(i)
			}
			if err := vec.Append(vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateS(e engine.Engine) {
	{
		var attrs []metadata.Attribute

		{
			attrs = append(attrs, metadata.Attribute{
				Name: "orderId",
				Type: types.Type{Oid: types.T_varchar, Size: 24},
			})
			attrs = append(attrs, metadata.Attribute{
				Name: "uid",
				Type: types.Type{Oid: types.T_varchar, Size: 24},
			})
			attrs = append(attrs, metadata.Attribute{
				Name: "price",
				Type: types.Type{Oid: types.T_float64, Size: 8, Width: 8},
			})
		}
		if err := e.Create("S", attrs); err != nil {
			log.Fatal(err)
		}
	}
	r, err := e.Relation("S")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"orderId", "uid", "price"})
		{
			{
				vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i*2))
				}
				if err := vec.Append(vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[0] = vec
			}
			{
				vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i%2))
				}
				if err := vec.Append(vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[1] = vec
			}
			{
				vec := vector.New(types.Type{Oid: types.T_float64, Size: 8, Width: 8})
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vec.Append(vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[2] = vec
			}
		}
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
	{
		bat := batch.New(true, []string{"orderId", "uid", "price"})
		{
			vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i*2))
			}
			if err := vec.Append(vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i%2))
			}
			if err := vec.Append(vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.New(types.Type{Oid: types.T_float64, Size: 8, Width: 8})
			vs := make([]float64, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = float64(i)
			}
			if err := vec.Append(vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
}
