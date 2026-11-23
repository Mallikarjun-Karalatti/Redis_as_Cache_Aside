package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// RunCacheAsideExample demonstrates the production-hardened cache-aside pattern:
// 1. Namespaced key generation (cache:billing:product:{id})
// 2. Reading through cache (Miss -> Hit)
// 3. Negative caching for missing records (Anti-Penetration Sentinel)
// 4. Invalidation on write (delete instead of trying to keep in sync)
// 5. Inspecting cached entries directly via redis-cli
func RunCacheAsideExample() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	primary := NewMockPrimaryStore(150 * time.Millisecond)

	// Production configuration
	cache, err := New(Config{
		Client:      client,
		Namespace:   "billing",          // Application namespace in shared Redis
		TTL:         30 * time.Second,   // Staleness bound
		NotFoundTTL: 5 * time.Second,    // Short sentinel TTL for non-existent records
		LockTTL:     2 * time.Second,    // Exceeds worst-case primary read latency (150ms)
	})
	if err != nil {
		log.Fatalf("cache creation failed: %v", err)
	}
	ctx := context.Background()

	log.Printf("--- Step 1: Read existing product 'p-001' (Cold read / Miss) ---")
	res1, _ := cache.Get(ctx, "p-001", primary.Read)
	log.Printf("Key=%s | hit=%v latency=%.2fms record=%v", res1.CacheKey, res1.Hit, res1.RedisLatencyMs, res1.Record)

	log.Printf("--- Step 2: Read product 'p-001' again (Cache Hit) ---")
	res2, _ := cache.Get(ctx, "p-001", primary.Read)
	log.Printf("Key=%s | hit=%v latency=%.2fms (Direct Redis hit!)", res2.CacheKey, res2.Hit, res2.RedisLatencyMs)

	log.Printf("--- Step 3: Read non-existent product 'p-999' (Missing record handling) ---")
	res3, _ := cache.Get(ctx, "p-999", primary.Read)
	log.Printf("First query for 'p-999': hit=%v is_not_found=%v (Sentinel cached for 5s)", res3.Hit, res3.IsNotFound)

	res4, _ := cache.Get(ctx, "p-999", primary.Read)
	log.Printf("Second query for 'p-999': hit=%v is_not_found=%v latency=%.2fms (Anti-penetration protected!)",
		res4.Hit, res4.IsNotFound, res4.RedisLatencyMs)

	log.Printf("--- Step 4: Invalidate on write (Don't try to keep in sync) ---")
	primary.UpdateField("p-001", "price_cents", "699")
	deleted, _ := cache.Invalidate(ctx, "p-001")
	log.Printf("Primary DB updated; Cache key invalidated (deleted=%v)", deleted)

	log.Printf("--- Step 5: Direct Redis Inspection Commands ---")
	fmt.Printf("\nTo inspect in your terminal, run:\n")
	fmt.Printf("  docker exec -it redis redis-cli HGETALL %s\n", cache.CacheKey("p-001"))
	fmt.Printf("  docker exec -it redis redis-cli TTL %s\n", cache.CacheKey("p-001"))
	fmt.Printf("  docker exec -it redis redis-cli HGETALL %s\n", cache.CacheKey("p-999"))
	fmt.Printf("  docker exec -it redis redis-cli TTL %s\n\n", cache.CacheKey("p-999"))
}
