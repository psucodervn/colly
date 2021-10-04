package cache

import (
	"context"
	"errors"
)

type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
}

var ErrNotFound = errors.New("not found")
