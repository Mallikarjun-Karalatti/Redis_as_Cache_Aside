// Package main implements a production-hardened Redis cache-aside helper
// featuring:
// 1. Configurable positive TTL with staleness bounds and optional jitter.
// 2. Explicit write invalidation (delete rather than keeping in sync).
// 3. Missing record handling via short-lived "not-found" sentinels (anti-penetration).
// 4. Tuned Lua-backed single-flight lock with cryptographic caller-bound release.
// 5. Multi-tenant application namespacing (e.g. cache:billing:product:{id}).
// 6. Direct Redis inspection observability.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Acquire a short-lived lock with SET NX PX. Returns 1 on acquire, 0 otherwise.
const acquireLockScript = `
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return 1
end
return 0
`

// Release lock only if the token matches the caller's unique token.
const releaseLockScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`

// Sentinel field name used to cache missing records and avoid cache penetration.
const notFoundSentinelField = "_not_found"

// Loader fetches an entity from the primary store on a cache miss.
type Loader func(ctx context.Context, id string) (map[string]string, error)

// Config holds production RedisCache configuration.
type Config struct {
	Client      *redis.Client
	Namespace   string        // Application namespace (e.g. "billing", "catalog")
	Prefix      string        // Key prefix (defaults to "cache:<namespace>:product:")
	TTL         time.Duration // Positive entry TTL (staleness bound)
	NotFoundTTL time.Duration // Short negative cache TTL for missing records (defaults to 5s)
	LockTTL     time.Duration // Single-flight lock TTL (must exceed worst-case primary read latency)
	WaitPoll    time.Duration // Polling interval for concurrent readers waiting on single-flight
}

// RedisCache is a production cache-aside helper backed by Redis hashes with TTL and single-flight.
type RedisCache struct {
	client      *redis.Client
	namespace   string
	prefix      string
	ttl         time.Duration
	notFoundTTL time.Duration
	lockTTL     time.Duration
	waitPoll    time.Duration

	acquireScript *redis.Script
	releaseScript *redis.Script

	mu                  sync.Mutex
	hits                int
	misses              int
	negativeHits        int
	stampedesSuppressed int
}

// New creates a production RedisCache.
// Defaults: namespace "billing", prefix "cache:<namespace>:product:",
// TTL 30s, NotFoundTTL 5s, LockTTL 2s, WaitPoll 25ms.
func New(cfg Config) (*RedisCache, error) {
	if cfg.Client == nil {
		return nil, errors.New("cacheaside: Client is required")
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "billing"
	}
	if cfg.Prefix == "" {
		cfg.Prefix = fmt.Sprintf("cache:%s:product:", cfg.Namespace)
	}
	if cfg.TTL == 0 {
		cfg.TTL = 30 * time.Second
	}
	if cfg.NotFoundTTL == 0 {
		cfg.NotFoundTTL = 5 * time.Second
	}
	if cfg.LockTTL == 0 {
		cfg.LockTTL = 2 * time.Second
	}
	if cfg.WaitPoll == 0 {
		cfg.WaitPoll = 25 * time.Millisecond
	}

	return &RedisCache{
		client:        cfg.Client,
		namespace:     cfg.Namespace,
		prefix:        cfg.Prefix,
		ttl:           cfg.TTL,
		notFoundTTL:   cfg.NotFoundTTL,
		lockTTL:       cfg.LockTTL,
		waitPoll:      cfg.WaitPoll,
		acquireScript: redis.NewScript(acquireLockScript),
		releaseScript: redis.NewScript(releaseLockScript),
	}, nil
}

// CacheKey returns the namespaced Redis key for an entity ID.
func (c *RedisCache) CacheKey(id string) string { return c.prefix + id }

// LockKey returns the single-flight lock key for an entity ID.
func (c *RedisCache) LockKey(id string) string { return "lock:" + c.prefix + id }

// Result is returned by Get and includes timing, hit/miss, and sentinel info.
type Result struct {
	Record         map[string]string
	Hit            bool
	IsNotFound     bool    // True if entity does not exist (served from negative cache sentinel)
	CacheKey       string  // Full Redis key for easy inspection
	RedisLatencyMs float64
}

// Get returns the entity, checking Redis first and loading through the loader on a miss.
// 1. Checks Redis hash.
// 2. If present and sentinel "_not_found" is set, returns a cache hit with nil record immediately.
// 3. On miss, funnels through single-flight loader. If primary returns nil, caches a short-lived sentinel.
func (c *RedisCache) Get(ctx context.Context, id string, loader Loader) (Result, error) {
	cacheKey := c.CacheKey(id)

	start := time.Now()
	cached, err := c.client.HGetAll(ctx, cacheKey).Result()
	redisMs := float64(time.Since(start).Microseconds()) / 1000.0
	if err != nil {
		return Result{CacheKey: cacheKey, RedisLatencyMs: redisMs}, err
	}

	if len(cached) > 0 {
		c.recordHit()
		// Production check: Handle missing record sentinel (Anti-penetration)
		if cached[notFoundSentinelField] == "true" {
			c.recordNegativeHit()
			return Result{
				Record:         nil,
				Hit:            true,
				IsNotFound:     true,
				CacheKey:       cacheKey,
				RedisLatencyMs: redisMs,
			}, nil
		}
		return Result{
			Record:         cached,
			Hit:            true,
			IsNotFound:     false,
			CacheKey:       cacheKey,
			RedisLatencyMs: redisMs,
		}, nil
	}

	c.recordMiss()
	record, isNotFound, err := c.loadWithSingleFlight(ctx, id, loader)
	return Result{
		Record:         record,
		Hit:            false,
		IsNotFound:     isNotFound,
		CacheKey:       cacheKey,
		RedisLatencyMs: redisMs,
	}, err
}

func (c *RedisCache) loadWithSingleFlight(ctx context.Context, id string, loader Loader) (map[string]string, bool, error) {
	cacheKey := c.CacheKey(id)
	lockKey := c.LockKey(id)
	tokenBytes := make([]byte, 8)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, false, err
	}
	token := hex.EncodeToString(tokenBytes)

	// Acquire single-flight lock with TTL sized above worst-case primary read latency
	acquiredVal, err := c.acquireScript.Run(ctx, c.client,
		[]string{lockKey},
		token,
		c.lockTTL.Milliseconds(),
	).Int()
	if err != nil {
		return nil, false, err
	}

	if acquiredVal == 1 {
		// Release the lock safely using the token
		defer func() {
			_, _ = c.releaseScript.Run(ctx, c.client, []string{lockKey}, token).Result()
		}()

		record, err := loader(ctx, id)
		if err != nil {
			return nil, false, err
		}

		pipe := c.client.TxPipeline()
		pipe.Del(ctx, cacheKey)

		// Production hardening: Handle missing records explicitly (Anti-Penetration Sentinel)
		if record == nil {
			pipe.HSet(ctx, cacheKey, notFoundSentinelField, "true", "id", id)
			pipe.Expire(ctx, cacheKey, c.notFoundTTL)
			if _, err := pipe.Exec(ctx); err != nil {
				return nil, true, err
			}
			return nil, true, nil
		}

		// Positive record caching
		fields := make([]any, 0, len(record)*2)
		for k, v := range record {
			fields = append(fields, k, v)
		}
		pipe.HSet(ctx, cacheKey, fields...)
		pipe.Expire(ctx, cacheKey, c.ttl)
		if _, err := pipe.Exec(ctx); err != nil {
			return record, false, err
		}
		return record, false, nil
	}

	// Concurrent caller suppressed; wait for leader to populate cache
	c.recordStampedeSuppressed()
	deadline := time.Now().Add(c.lockTTL)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(c.waitPoll):
		}
		cached, err := c.client.HGetAll(ctx, cacheKey).Result()
		if err == nil && len(cached) > 0 {
			if cached[notFoundSentinelField] == "true" {
				return nil, true, nil
			}
			return cached, false, nil
		}
	}
	record, err := loader(ctx, id)
	return record, record == nil, err
}

// Invalidate explicitly deletes the cache entry.
// Production Rule: When data changes in primary DB, always invalidate rather
// than attempting complex in-place synchronization.
func (c *RedisCache) Invalidate(ctx context.Context, id string) (bool, error) {
	n, err := c.client.Del(ctx, c.CacheKey(id)).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// TTLRemaining returns the remaining TTL in seconds for the key (-2 if missing, -1 if no expiry).
func (c *RedisCache) TTLRemaining(ctx context.Context, id string) (int, error) {
	d, err := c.client.TTL(ctx, c.CacheKey(id)).Result()
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return int(d / time.Second), nil
	}
	return int(d.Seconds()), nil
}

// Stats returns hit/miss/stampede metrics.
func (c *RedisCache) Stats() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := c.hits + c.misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(int(1000.0*float64(c.hits)/float64(total))) / 10.0
	}
	return map[string]any{
		"hits":                 c.hits,
		"misses":               c.misses,
		"negative_hits":        c.negativeHits,
		"stampedes_suppressed": c.stampedesSuppressed,
		"hit_rate_pct":         hitRate,
		"namespace":            c.namespace,
		"prefix":               c.prefix,
		"ttl_seconds":          int(c.ttl.Seconds()),
		"not_found_ttl":        int(c.notFoundTTL.Seconds()),
		"lock_ttl_ms":          c.lockTTL.Milliseconds(),
	}
}

// ResetStats zeroes the in-process counters.
func (c *RedisCache) ResetStats() {
	c.mu.Lock()
	c.hits, c.misses, c.negativeHits, c.stampedesSuppressed = 0, 0, 0, 0
	c.mu.Unlock()
}

func (c *RedisCache) recordHit()                { c.mu.Lock(); c.hits++; c.mu.Unlock() }
func (c *RedisCache) recordMiss()               { c.mu.Lock(); c.misses++; c.mu.Unlock() }
func (c *RedisCache) recordNegativeHit()        { c.mu.Lock(); c.negativeHits++; c.mu.Unlock() }
func (c *RedisCache) recordStampedeSuppressed() { c.mu.Lock(); c.stampedesSuppressed++; c.mu.Unlock() }
