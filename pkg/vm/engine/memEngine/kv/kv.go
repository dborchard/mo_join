package kv

import (
	"errors"
	"mo_join/pkg/vm/mempool"
	"mo_join/pkg/vm/process"
)

var (
	NotExist = errors.New("not exist")
)

type KV struct {
	mp map[string][]byte
}

func New() *KV {
	return &KV{make(map[string][]byte)}
}

func (a *KV) Close() error {
	return nil
}

func (a *KV) Del(k string) error {
	delete(a.mp, k)
	return nil
}

func (a *KV) Set(k string, v []byte) error {
	a.mp[k] = v
	return nil
}

func (a *KV) Get(k string, proc *process.Process) ([]byte, error) {
	v, ok := a.mp[k]
	if !ok {
		return nil, NotExist
	}
	data, err := proc.Alloc(int64(len(v)) + mempool.HeaderSize)
	if err != nil {
		return nil, err
	}
	copy(data[mempool.HeaderSize:], v)
	return data[:len(v)+mempool.HeaderSize], nil
}
