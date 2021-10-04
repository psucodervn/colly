// Modified from https://github.com/gocolly/redisstorage/blob/master/redisstorage.go

package storage

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisStorage implements the redis storage backend for Colly
type RedisStorage struct {
	// Address is the redis server address
	Address string
	// Password is the password for the redis server
	Password string
	// DB is the redis database. Default is 0
	DB int
	// Prefix is an optional string in the keys. It can be used
	// to use one redis database for independent scraping tasks.
	Prefix string
	// Client is the redis connection
	Client redis.UniversalClient

	// Expiration time for Visited keys. After expiration pages
	// are to be visited again.
	Expires time.Duration

	mu sync.RWMutex // Only used for cookie methods.
}

// Init initializes the redis storage
func (s *RedisStorage) Init() error {
	if s.Client == nil {
		s.Client = redis.NewClient(&redis.Options{
			Addr:     s.Address,
			Password: s.Password,
			DB:       s.DB,
		})
	}
	_, err := s.Client.Ping(context.Background()).Result()
	if err != nil {
		return fmt.Errorf("Redis connection error: %s", err.Error())
	}
	return err
}

// Clear removes all entries from the storage
func (s *RedisStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := context.Background()
	r := s.Client.Keys(ctx, s.getCookieID("*"))
	keys, err := r.Result()
	if err != nil {
		return err
	}
	r2 := s.Client.Keys(ctx, s.Prefix+":request:*")
	keys2, err := r2.Result()
	if err != nil {
		return err
	}
	keys = append(keys, keys2...)
	keys = append(keys, s.getQueueID())
	return s.Client.Del(ctx, keys...).Err()
}

// Visited implements colly/storage.Visited()
func (s *RedisStorage) Visited(requestID uint64) error {
	ctx := context.Background()
	return s.Client.Set(ctx, s.getIDStr(requestID), "1", s.Expires).Err()
}

// IsVisited implements colly/storage.IsVisited()
func (s *RedisStorage) IsVisited(requestID uint64) (bool, error) {
	ctx := context.Background()
	_, err := s.Client.Get(ctx, s.getIDStr(requestID)).Result()
	if err == redis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// SetCookies implements colly/storage..SetCookies()
func (s *RedisStorage) SetCookies(u *url.URL, cookies string) {
	// TODO(js) Cookie methods currently have no way to return an error.

	// We need to use a write lock to prevent a race in the db:
	// if two callers set cookies in a very small window of time,
	// it is possible to drop the new cookies from one caller
	// ('last update wins' == best avoided).
	s.mu.Lock()
	defer s.mu.Unlock()
	// return s.Client.Set(s.getCookieID(u.Host), stringify(cnew), 0).Err()
	ctx := context.Background()
	err := s.Client.Set(ctx, s.getCookieID(u.Host), cookies, 0).Err()
	if err != nil {
		// return nil
		log.Printf("SetCookies() .Set error %s", err)
		return
	}
}

// Cookies implements colly/storage.Cookies()
func (s *RedisStorage) Cookies(u *url.URL) string {
	// TODO(js) Cookie methods currently have no way to return an error.

	s.mu.RLock()
	ctx := context.Background()
	cookiesStr, err := s.Client.Get(ctx, s.getCookieID(u.Host)).Result()
	s.mu.RUnlock()
	if err == redis.Nil {
		cookiesStr = ""
	} else if err != nil {
		// return nil, err
		log.Printf("Cookies() .Get error %s", err)
		return ""
	}
	return cookiesStr
}

// AddRequest implements queue.RedisStorage.AddRequest() function
func (s *RedisStorage) AddRequest(r []byte) error {
	ctx := context.Background()
	return s.Client.RPush(ctx, s.getQueueID(), r).Err()
}

// GetRequest implements queue.RedisStorage.GetRequest() function
func (s *RedisStorage) GetRequest() ([]byte, error) {
	ctx := context.Background()
	r, err := s.Client.LPop(ctx, s.getQueueID()).Bytes()
	if err != nil {
		return nil, err
	}
	return r, err
}

// QueueSize implements queue.RedisStorage.QueueSize() function
func (s *RedisStorage) QueueSize() (int, error) {
	ctx := context.Background()
	i, err := s.Client.LLen(ctx, s.getQueueID()).Result()
	return int(i), err
}

func (s *RedisStorage) getIDStr(ID uint64) string {
	return fmt.Sprintf("%s:request:%d", s.Prefix, ID)
}

func (s *RedisStorage) getCookieID(c string) string {
	return fmt.Sprintf("%s:cookie:%s", s.Prefix, c)
}

func (s *RedisStorage) getQueueID() string {
	return fmt.Sprintf("%s:queue", s.Prefix)
}
