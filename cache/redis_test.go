package cache

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestRedis(t *testing.T) {
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   8,
	})

	expiration := 2 * time.Second
	str := "Why doesn't colly support redis cache?"
	key := "colly"
	ctx := context.Background()

	c := NewRedisCache(cli, "redis:cache", expiration)
	if err := c.Put(ctx, key, []byte(str)); err != nil {
		t.Fatalf("Error putting string in cache: %v", err)
		return
	}

	res, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("Error getting string from cache: %v", err)
		return
	}
	if string(res) != str {
		t.Fatalf("Expected %s, got %s", str, res)
		return
	}

	time.Sleep(2 * time.Second)
	if _, err := c.Get(ctx, key); err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, got %v", err)
		return
	}
}
