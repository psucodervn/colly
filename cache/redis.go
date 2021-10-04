package cache

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisCache struct {
	cli        redis.UniversalClient
	prefix     string
	expiration time.Duration
}

func NewRedisCache(client redis.UniversalClient, prefix string, expiration time.Duration) *RedisCache {
	return &RedisCache{
		cli:        client,
		prefix:     prefix,
		expiration: expiration,
	}
}

func (c *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := c.cli.Get(ctx, c.getKey(key)).Bytes()
	if err == redis.Nil {
		return nil, ErrNotFound
	}
	return b, err
}

func (c *RedisCache) Put(ctx context.Context, key string, value []byte) error {
	return c.cli.SetEX(ctx, c.getKey(key), value, c.expiration).Err()
}

func (c *RedisCache) getKey(key string) string {
	return c.prefix + ":" + key
}
