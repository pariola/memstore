package store

import (
	"context"
)

type basic struct {
	kv map[string][]byte
}

func Basic() *basic {
	return &basic{
		kv: make(map[string][]byte),
	}
}

func (b *basic) Set(_ context.Context, key string, value []byte) error {
	b.kv[key] = value
	return nil
}

func (b *basic) Get(_ context.Context, key string) ([]byte, error) {
	v, ok := b.kv[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return v, nil
}

func (b *basic) Del(_ context.Context, key string) error {
	_, ok := b.kv[key]
	if !ok {
		return ErrKeyNotFound
	}

	delete(b.kv, key)
	return nil
}
