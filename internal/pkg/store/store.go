package store

import (
	"context"
	"errors"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

type Store interface {
	Set(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Del(ctx context.Context, key string) error
}
