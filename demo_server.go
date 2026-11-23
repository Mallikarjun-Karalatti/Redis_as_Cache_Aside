// Redis Cache-Aside Production Demo Server
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type demoServer struct {
	cache   *RedisCache
	primary *MockPrimaryStore
}

// RunDemoServer parses CLI flags and starts the cache-aside demo HTTP server.
func RunDemoServer() {
	port := flag.Int("port", 8080, "HTTP bind port")
	host := flag.String("host", "127.0.0.1", "HTTP bind host")
	redisHost := flag.String("redis-host", "localhost", "Redis host")
	redisPort := flag.Int("redis-port", 6379, "Redis port")
	namespace := flag.String("namespace", "billing", "Application namespace in shared Redis deployment")
	ttl := flag.Int("ttl", 30, "Cache positive TTL in seconds")
	notFoundTTL := flag.Int("not-found-ttl", 5, "Missing record sentinel TTL in seconds")
	lockTTLMs := flag.Int("lock-ttl-ms", 2000, "Single-flight lock TTL in ms (tuned > primary latency)")
	primaryLatencyMs := flag.Int("primary-latency-ms", 150, "Simulated primary latency in ms")
	flag.Parse()

	client := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", *redisHost, *redisPort),
	})
	defer client.Close()

	if err := client.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("connect Redis at %s:%d: %v", *redisHost, *redisPort, err)
	}

	cache, err := New(Config{
		Client:      client,
		Namespace:   *namespace,
		TTL:         time.Duration(*ttl) * time.Second,
		NotFoundTTL: time.Duration(*notFoundTTL) * time.Second,
		LockTTL:     time.Duration(*lockTTLMs) * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("create cache: %v", err)
	}
	primary := NewMockPrimaryStore(time.Duration(*primaryLatencyMs) * time.Millisecond)

	s := &demoServer{cache: cache, primary: primary}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/products", s.handleProducts)
	mux.HandleFunc("/read", s.handleRead)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/invalidate", s.handleInvalidate)
	mux.HandleFunc("/update", s.handleUpdate)
	mux.HandleFunc("/stampede", s.handleStampede)
	mux.HandleFunc("/reset", s.handleReset)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	log.Printf("====================================================================")
	log.Printf("Redis Cache-Aside Production Demo listening on http://%s", addr)
	log.Printf("• Shared Redis Namespace: '%s' (Prefix: '%s')", *namespace, cache.prefix)
	log.Printf("• Positive Cache TTL: %d s | Negative (Missing) Sentinel TTL: %d s", *ttl, *notFoundTTL)
	log.Printf("• Single-Flight Lock TTL: %d ms | Primary DB Latency: %d ms", *lockTTLMs, *primaryLatencyMs)
	log.Printf("====================================================================")
	log.Fatal(http.ListenAndServe(addr, mux))
}

func (s *demoServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	page := htmlPage(
		s.primary.ListIDs(),
		s.cache.namespace,
		int(s.primary.ReadLatency/time.Millisecond),
		int(s.cache.ttl.Seconds()),
		int(s.cache.notFoundTTL.Seconds()),
		int(s.cache.lockTTL.Milliseconds()),
	)
	_, _ = w.Write([]byte(page))
}

func (s *demoServer) handleProducts(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"products": s.primary.ListIDs()})
}

func (s *demoServer) handleRead(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Missing 'id' query parameter."})
		return
	}
	start := time.Now()
	res, err := s.cache.Get(r.Context(), id, s.primary.Read)
	totalMs := float64(time.Since(start).Microseconds()) / 1000.0
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	ttl, _ := s.cache.TTLRemaining(r.Context(), id)
	cacheKey := s.cache.CacheKey(id)

	writeJSON(w, http.StatusOK, map[string]any{
		"id":               id,
		"record":           res.Record,
		"is_not_found":     res.IsNotFound,
		"hit":              res.Hit,
		"cache_key":        cacheKey,
		"redis_latency_ms": round2(res.RedisLatencyMs),
		"total_latency_ms": round2(totalMs),
		"ttl_remaining":    ttl,
		"inspect_hgetall":  fmt.Sprintf("redis-cli HGETALL %s", cacheKey),
		"inspect_ttl":      fmt.Sprintf("redis-cli TTL %s", cacheKey),
		"stats":            s.buildStats(),
	})
}

func (s *demoServer) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.buildStats())
}

func (s *demoServer) handleInvalidate(w http.ResponseWriter, r *http.Request) {
	form, err := parseForm(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	id := form.Get("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Missing 'id'."})
		return
	}
	deleted, err := s.cache.Invalidate(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":        id,
		"deleted":   deleted,
		"cache_key": s.cache.CacheKey(id),
		"stats":     s.buildStats(),
	})
}

func (s *demoServer) handleUpdate(w http.ResponseWriter, r *http.Request) {
	form, err := parseForm(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	id := form.Get("id")
	field := form.Get("field")
	value := form.Get("value")
	if id == "" || field == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Missing 'id' or 'field'."})
		return
	}
	if !s.primary.UpdateField(id, field, value) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "Unknown product."})
		return
	}
	// Production pattern: Delete cache key on write, don't try to keep in sync.
	if _, err := s.cache.Invalidate(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":        id,
		"field":     field,
		"value":     value,
		"cache_key": s.cache.CacheKey(id),
		"stats":     s.buildStats(),
	})
}

func (s *demoServer) handleStampede(w http.ResponseWriter, r *http.Request) {
	form, err := parseForm(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	id := form.Get("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Missing 'id'."})
		return
	}
	concurrency, _ := strconv.Atoi(form.Get("concurrency"))
	if concurrency < 2 {
		concurrency = 20
	}
	if concurrency > 50 {
		concurrency = 50
	}

	if _, err := s.cache.Invalidate(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	primaryBefore := s.primary.Reads()

	results := make([]map[string]any, concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			reqStart := time.Now()
			res, err := s.cache.Get(context.Background(), id, s.primary.Read)
			elapsed := float64(time.Since(reqStart).Microseconds()) / 1000.0
			results[idx] = map[string]any{
				"hit":        res.Hit,
				"latency_ms": round2(elapsed),
				"err":        err != nil,
			}
		}(i)
	}
	wg.Wait()
	elapsedMs := float64(time.Since(start).Microseconds()) / 1000.0
	primaryReads := s.primary.Reads() - primaryBefore

	writeJSON(w, http.StatusOK, map[string]any{
		"id":            id,
		"cache_key":     s.cache.CacheKey(id),
		"concurrency":   concurrency,
		"elapsed_ms":    round2(elapsedMs),
		"primary_reads": primaryReads,
		"results":       results,
		"stats":         s.buildStats(),
	})
}

func (s *demoServer) handleReset(w http.ResponseWriter, _ *http.Request) {
	s.cache.ResetStats()
	s.primary.ResetReads()
	writeJSON(w, http.StatusOK, s.buildStats())
}

func (s *demoServer) buildStats() map[string]any {
	stats := s.cache.Stats()
	stats["primary_reads_total"] = s.primary.Reads()
	stats["primary_read_latency_ms"] = int(s.primary.ReadLatency / time.Millisecond)
	return stats
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func parseForm(r *http.Request) (form, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	return form(r.PostForm), nil
}

type form map[string][]string

func (f form) Get(key string) string {
	if v, ok := f[key]; ok && len(v) > 0 {
		return v[0]
	}
	return ""
}

func round2(v float64) float64 {
	return float64(int(v*100)) / 100.0
}

func htmlPage(productIDs []string, namespace string, primaryLatencyMs, cacheTTL, notFoundTTL, lockTTL int) string {
	var opts strings.Builder
	for _, id := range productIDs {
		safe := html.EscapeString(id)
		fmt.Fprintf(&opts, `<option value="%s">%s</option>`, safe, safe)
	}
	// Add an option to test missing products for negative caching
	opts.WriteString(`<option value="p-missing-999">p-missing-999 (Missing Product / Sentinel Test)</option>`)

	page := strings.Replace(htmlTemplate, "{{OPTIONS}}", opts.String(), 1)
	page = strings.Replace(page, "{{NAMESPACE}}", html.EscapeString(namespace), -1)
	page = strings.Replace(page, "{{PRIMARY_LATENCY}}", strconv.Itoa(primaryLatencyMs), -1)
	page = strings.Replace(page, "{{CACHE_TTL}}", strconv.Itoa(cacheTTL), -1)
	page = strings.Replace(page, "{{NOT_FOUND_TTL}}", strconv.Itoa(notFoundTTL), -1)
	page = strings.Replace(page, "{{LOCK_TTL}}", strconv.Itoa(lockTTL), -1)
	return page
}

const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Redis Cache-Aside Production Demo</title>
  <style>
    :root { --bg:#f6f1e8;--panel:#fffaf2;--ink:#1f2933;--accent:#b8572f;--accent-dark:#8f421f;
      --muted:#5d6b75;--line:#e7d9c6;--ok:#d7f0de;--warn:#fbe7e0;--hit:#c9e7d2;--miss:#f5d6c6; }
    * { box-sizing:border-box; }
    body { margin:0;font-family:Georgia,"Times New Roman",serif;color:var(--ink);
      background:radial-gradient(circle at top left,#fff7ea,transparent 32rem),
        linear-gradient(180deg,#f3ecdf 0%,var(--bg) 100%);min-height:100vh; }
    main { max-width:1040px;margin:0 auto;padding:40px 20px 72px; }
    h1 { font-size:clamp(2rem,4.5vw,3.4rem);line-height:1.1;margin-bottom:12px; }
    p.lede { max-width:56rem;font-size:1.05rem;color:var(--muted);line-height:1.5; }
    .badges-row { display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px; }
    .pill { display:inline-block;border-radius:999px;background:#efe2cf;color:var(--accent-dark);
      padding:6px 12px;font-size:0.85rem;font-weight:600;font-family:system-ui,sans-serif; }
    .grid { display:grid;gap:20px;grid-template-columns:repeat(auto-fit,minmax(310px,1fr));margin-top:24px; }
    .panel { background:rgba(255,250,242,0.95);border:1px solid var(--line);border-radius:18px;
      padding:22px;box-shadow:0 12px 40px rgba(105,74,45,0.06); }
    .panel h2 { margin-top:0;margin-bottom:10px;font-size:1.35rem; }
    label { display:block;font-weight:bold;margin:12px 0 6px;font-size:0.95rem; }
    input,select { width:100%;padding:10px 12px;border-radius:10px;border:1px solid #cfbca6;font:inherit;background:white; }
    button { appearance:none;border:0;border-radius:999px;background:var(--accent);color:white;
      padding:10px 18px;font:inherit;font-size:0.95rem;cursor:pointer;margin-right:8px;margin-top:12px; }
    button.secondary { background:#38424a; }
    button:hover { background:var(--accent-dark); }
    button.secondary:hover { background:#20282e; }
    dl { display:grid;grid-template-columns:max-content 1fr;gap:6px 14px;margin:0;font-size:0.95rem; }
    dt { font-weight:bold;color:#444; } dd { margin:0;word-break:break-word; }
    .badge { display:inline-block;border-radius:6px;padding:3px 8px;font-size:0.85rem;font-weight:bold;font-family:system-ui,sans-serif; }
    .badge.hit { background:var(--hit);color:#1d4a2c; }
    .badge.miss { background:var(--miss);color:#6b3220; }
    .badge.sentinel { background:#e1d4f2;color:#4a2478; }
    .badge.notfound { background:var(--warn);color:#a83214; }
    .cli-box { background:#222831;color:#eee;padding:12px 14px;border-radius:10px;margin-top:12px;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:0.85rem;line-height:1.6;overflow-x:auto; }
    .cli-box code { color:#00adb5; }
    #status { margin-top:20px;padding:14px 16px;border-radius:14px;display:none;font-weight:500; }
    #status.ok { display:block;background:var(--ok); }
    #status.error { display:block;background:var(--warn); }
    @media (max-width:600px){ main{padding-top:20px;} button{width:100%;} }
  </style>
</head>
<body>
  <main>
    <div class="badges-row">
      <span class="pill">Namespace: {{NAMESPACE}}</span>
      <span class="pill">Positive TTL: {{CACHE_TTL}}s</span>
      <span class="pill">Missing Sentinel TTL: {{NOT_FOUND_TTL}}s</span>
      <span class="pill">Lock TTL: {{LOCK_TTL}}ms</span>
    </div>
    <h1>Production Redis Cache-Aside</h1>
    <p class="lede">
      Production-hardened implementation: multi-tenant application namespacing (<code>cache:{{NAMESPACE}}:product:{id}</code>),
      anti-penetration missing-record sentinels ({{NOT_FOUND_TTL}}s TTL), write-invalidation (never rewrite stale cache),
      tuned single-flight locks ({{LOCK_TTL}}ms), and direct <code>redis-cli</code> inspection.
    </p>

    <div class="grid">
      <section class="panel">
        <h2>Read a Product</h2>
        <label for="product-id">Product ID</label>
        <select id="product-id">{{OPTIONS}}</select>
        <button id="read-button">Read through Cache</button>
        <button id="invalidate-button" class="secondary">Invalidate Cache</button>
        <p style="font-size:0.9rem;color:var(--muted);margin-top:10px;">
          Select <strong>p-001</strong> for valid product, or <strong>p-missing-999</strong> to test anti-penetration missing record caching.
        </p>
      </section>

      <section class="panel">
        <h2>Invalidate on Write</h2>
        <p style="font-size:0.9rem;color:var(--muted);">
          Rule: When data changes, delete the cache key rather than keeping it in sync. The next read repopulates cleanly.
        </p>
        <label for="update-field">Field</label>
        <select id="update-field">
          <option value="name">name</option>
          <option value="price_cents">price_cents</option>
          <option value="stock">stock</option>
        </select>
        <label for="update-value">New value</label>
        <input id="update-value" value="699">
        <button id="update-button">Update Primary & Invalidate</button>
      </section>

      <section class="panel">
        <h2>Stampede Protection</h2>
        <p style="font-size:0.9rem;color:var(--muted);">
          Fires concurrent reads at a cold key. Lua-backed single-flight lock ensures only 1 request queries the primary DB.
        </p>
        <label for="stampede-concurrency">Concurrent Readers</label>
        <input id="stampede-concurrency" type="number" value="20" min="2" max="50">
        <button id="stampede-button">Run Stampede Test</button>
      </section>

      <section class="panel">
        <h2>Metrics & Telemetry</h2>
        <div id="stats-view">Loading...</div>
        <button id="reset-button" class="secondary">Reset Counters</button>
      </section>

      <section class="panel" style="grid-column: 1 / -1;">
        <h2>Last Query Result & Direct Redis Inspection</h2>
        <div id="result-view"><p>Read a product above to inspect the cached Redis record, timing, and direct redis-cli commands.</p></div>
      </section>
    </div>

    <div id="status"></div>
  </main>

  <script>
    const productSelect = document.getElementById("product-id");
    const updateField = document.getElementById("update-field");
    const updateValue = document.getElementById("update-value");
    const stampedeConcurrency = document.getElementById("stampede-concurrency");
    const statsView = document.getElementById("stats-view");
    const resultView = document.getElementById("result-view");
    const statusBox = document.getElementById("status");

    function setStatus(message, kind) { statusBox.textContent = message; statusBox.className = kind; }
    function escapeHtml(val) {
      return String(val ?? "").replace(/[&<>"']/g, (c) =>
        ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
    }

    function renderStats(stats) {
      if (!stats) { statsView.textContent = "(no data)"; return; }
      statsView.innerHTML = ` + "`" + `<dl>
        <dt>Hits</dt><dd>${stats.hits} (including ${stats.negative_hits ?? 0} missing-record sentinel hits)</dd>
        <dt>Misses</dt><dd>${stats.misses}</dd>
        <dt>Hit Rate</dt><dd><strong>${stats.hit_rate_pct}%</strong></dd>
        <dt>Stampedes Suppressed</dt><dd>${stats.stampedes_suppressed}</dd>
        <dt>Primary DB Reads</dt><dd>${stats.primary_reads_total}</dd>
        <dt>Primary Read Latency</dt><dd>${stats.primary_read_latency_ms} ms</dd>
        <dt>Namespace</dt><dd><code>${stats.namespace}</code></dd>
      </dl>` + "`" + `;
    }

    function renderRead(data) {
      if (!data) { resultView.innerHTML = "<p>(no response)</p>"; return; }
      let badge = "";
      let recordHtml = "";

      if (data.is_not_found) {
        badge = '<span class="badge notfound">404 NOT FOUND (Sentinel Cached)</span>';
        recordHtml = ` + "`" + `<p style="color:#b8572f;margin-top:8px;">
          🛡️ <strong>Anti-Penetration Sentinel:</strong> This missing record was cached in Redis with a short TTL (${data.ttl_remaining}s).
          Subsequent requests are served directly from Redis in <strong>${data.redis_latency_ms} ms</strong> without querying the primary database!
        </p>` + "`" + `;
      } else {
        badge = data.hit
          ? '<span class="badge hit">CACHE HIT</span>'
          : '<span class="badge miss">CACHE MISS</span>';
        const r = data.record || {};
        recordHtml = ` + "`" + `<dl style="margin-top:10px;">
          <dt>id</dt><dd>${escapeHtml(r.id)}</dd>
          <dt>name</dt><dd>${escapeHtml(r.name)}</dd>
          <dt>price_cents</dt><dd>${escapeHtml(r.price_cents)}</dd>
          <dt>stock</dt><dd>${escapeHtml(r.stock)}</dd>
        </dl>` + "`" + `;
      }

      resultView.innerHTML = ` + "`" + `
        <div style="margin-bottom:12px;">
          ${badge} &nbsp;
          Redis Read Latency: <strong>${data.redis_latency_ms} ms</strong> &nbsp; | &nbsp;
          Total Request Time: <strong>${data.total_latency_ms} ms</strong> &nbsp; | &nbsp;
          TTL Remaining: <strong>${data.ttl_remaining} s</strong>
        </div>
        ${recordHtml}
        <div class="cli-box">
          <strong>Inspect directly in Redis (run in your terminal):</strong><br>
          docker exec -it redis <code>${escapeHtml(data.inspect_hgetall)}</code><br>
          docker exec -it redis <code>${escapeHtml(data.inspect_ttl)}</code>
        </div>
      ` + "`" + `;
    }

    function renderStampede(data) {
      const hits = data.results.filter((r) => r.hit).length;
      const misses = data.results.length - hits;
      resultView.innerHTML = ` + "`" + `
        <p>Fired <strong>${data.concurrency}</strong> concurrent reads in <strong>${data.elapsed_ms}</strong> ms.</p>
        <p>Cache misses: <strong>${misses}</strong> &nbsp; | &nbsp;
           Cache hits: <strong>${hits}</strong> &nbsp; | &nbsp;
           Primary DB reads: <strong>${data.primary_reads}</strong></p>
        <p style="color:#2a6e3f;font-weight:500;">
          ✅ Single-flight lock protected the primary store: only 1 request reached the database; all other ${data.concurrency - 1} callers waited and were served from cache!
        </p>
        <div class="cli-box">
          <strong>Inspect key:</strong><br>
          docker exec -it redis <code>redis-cli HGETALL ${escapeHtml(data.cache_key)}</code>
        </div>
      ` + "`" + `;
    }

    async function loadStats() {
      try {
        const res = await fetch("/stats");
        renderStats(await res.json());
      } catch (e) {}
    }

    document.getElementById("read-button").addEventListener("click", async () => {
      const id = productSelect.value;
      const r = await fetch(` + "`" + `/read?id=${encodeURIComponent(id)}` + "`" + `);
      const d = await r.json();
      if (!r.ok) { setStatus(d.error || "Read failed.", "error"); return; }
      renderRead(d);
      renderStats(d.stats);
      if (d.is_not_found) {
        setStatus(d.hit ? "Missing record served from Redis sentinel." : "Missing record verified on primary; sentinel cached for 5s.", "ok");
      } else {
        setStatus(d.hit ? "Served from Redis cache." : "Loaded from primary database and cached.", "ok");
      }
    });

    document.getElementById("invalidate-button").addEventListener("click", async () => {
      const id = productSelect.value;
      const r = await fetch("/invalidate", { method: "POST", body: new URLSearchParams({ id }) });
      const d = await r.json();
      renderStats(d.stats);
      setStatus(d.deleted ? ` + "`" + `Cache key '${d.cache_key}' deleted from Redis.` + "`" + ` : "No cache key found to delete.", "ok");
    });

    document.getElementById("update-button").addEventListener("click", async () => {
      const id = productSelect.value;
      const r = await fetch("/update", { method: "POST",
        body: new URLSearchParams({ id, field: updateField.value, value: updateValue.value }) });
      const d = await r.json();
      if (!r.ok) { setStatus(d.error || "Update failed.", "error"); return; }
      renderStats(d.stats);
      setStatus(` + "`" + `Primary DB updated; Cache key '${d.cache_key}' invalidated.` + "`" + `, "ok");
    });

    document.getElementById("stampede-button").addEventListener("click", async () => {
      const id = productSelect.value;
      setStatus("Running stampede test with single-flight Lua lock...", "ok");
      const r = await fetch("/stampede", { method: "POST",
        body: new URLSearchParams({ id, concurrency: stampedeConcurrency.value }) });
      const d = await r.json();
      if (!r.ok) { setStatus(d.error || "Test failed.", "error"); return; }
      renderStampede(d);
      renderStats(d.stats);
      setStatus(` + "`" + `Stampede complete: ${d.primary_reads} primary read for ${d.concurrency} concurrent callers.` + "`" + `, "ok");
    });

    document.getElementById("reset-button").addEventListener("click", async () => {
      const r = await fetch("/reset", { method: "POST" });
      renderStats(await r.json());
      setStatus("Metrics and counters reset.", "ok");
    });

    loadStats();
  </script>
</body>
</html>
`
