package main
import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"math/rand"
	"sync/atomic"
)

// -------------------------------
// Types (what we read/write)
// -------------------------------

// ItemResult is the per-ID result we keep. We don't assume a schema for the API's JSON,
// so we store the raw payload. You can pretty-print or post-process later.
type ItemResult struct {
	ID       string          `json:"id"`
	OK       bool            `json:"ok"`
	Status   int             `json:"status"`
	Error    string          `json:"error,omitempty"`
	Received json.RawMessage `json:"received,omitempty"`
}

// CacheFile is a JSON object { "id": ItemResult, ... } persisted across runs.
type CacheFile map[string]ItemResult

// -------------------------------
// CLI flags
// -------------------------------

var (
	flagIDs         = flag.String("ids", "", "Comma-separated list of item IDs (e.g., \"b6589fc6,356a192b\"). If empty, uses built-in example IDs.")
	flagIDsFile     = flag.String("ids_file", "", "Optional path to a text file with one item ID per line.")
	flagClientID    = flag.String("client_id", "eluvio", "Client ID to send as required query param.")
	flagOut         = flag.String("out", "results.json", "Path to write fetched results as JSON.")
	flagCache       = flag.String("cache", "cache.json", "Path to a JSON cache file (persisted across runs).")
	flagMaxConc     = flag.Int("max_concurrency", 5, "Maximum number of simultaneous requests (MUST be ≤ 5).")
	flagMaxRetries  = flag.Int("max_retries", 2, "Number of retries for transient errors (e.g., 429, 5xx).")
	flagHTTPTimeout = flag.Duration("http_timeout", 15*time.Second, "Per-request timeout.")
)

// -------------------------------
// Constants
// -------------------------------

const baseURL = "https://wallet.contentfabric.io/items"

// -------------------------------
// Helpers: I/O
// -------------------------------

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func loadCache(path string) (CacheFile, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return CacheFile{}, nil // no cache yet (that's fine)
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	cache := CacheFile{}
	if err := dec.Decode(&cache); err != nil {
		return nil, fmt.Errorf("failed to parse cache %s: %w", path, err)
	}
	return cache, nil
}

func saveCache(path string, cache CacheFile) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cache); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func writeResults(path string, results []ItemResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}

// -------------------------------
// Helpers: IDs input/dedup
// -------------------------------

func parseIDsFromFlags() ([]string, error) {
	var ids []string

	// 1) From file (one per line)
	if *flagIDsFile != "" {
		data, err := os.ReadFile(*flagIDsFile)
		if err != nil {
			return nil, fmt.Errorf("read ids_file: %w", err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line != "" {
				ids = append(ids, line)
			}
		}
	}

	// 2) From -ids (comma-separated)
	if *flagIDs != "" {
		for _, part := range strings.Split(*flagIDs, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				ids = append(ids, part)
			}
		}
	}

	// 3) Default to the provided example IDs if nothing was supplied
	if len(ids) == 0 {
		ids = []string{
			"b6589fc6", "356a192b", "da4b9237", "77de68da", "1b645389", "ac3478d6",
			"c1dfd96e", "902ba3cd", "fe5dbbce", "0ade7c2c", "b1d57811", "17ba0791",
			"7b52009b", "bd307a3e", "fa35e192", "f1abd670", "1574bddb", "0716d970",
			"9e6a55b6", "b3f0c7f6", "91032ad7", "472b07b9", "12c6fc06", "d435a6cd",
			"4d134bc0", "f6e1126c", "887309d0", "bc33ea4e", "0a57cb53", "7719a1c7",
			"22d200f8", "63266754", "cb4e5208", "b6692ea5", "f1f836cb", "972a67c4",
			"fc074d50", "cb7a1d77", "5b384ce3", "ca3512f4", "af3e1334", "761f22b2",
			"92cfceb3", "0286dd55", "98fbc42f", "fb644351", "fe2ef495", "827bfc45",
			"64e095fe", "2e01e174",
		}
	}

	// Deduplicate (preserve first appearance order)
	seen := make(map[string]bool)
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out, nil
}

// -------------------------------
// Core: HTTP fetch with limit, retry, and backoff
// -------------------------------

// fetchOne performs the GET with required query params:
//   GET https://wallet.contentfabric.io/items/:id?authorization=<base64(id)>&client_id=<client_id>
func fetchOne(ctx context.Context, client *http.Client, id, clientID string) ItemResult {
	auth := base64.StdEncoding.EncodeToString([]byte(id))
	url := fmt.Sprintf("%s/%s?authorization=%s&client_id=%s", baseURL, id, auth, clientID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return ItemResult{ID: id, OK: false, Status: 0, Error: err.Error()}
	}

	resp, err := client.Do(req)
	if err != nil {
		return ItemResult{ID: id, OK: false, Status: 0, Error: err.Error()}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Save raw JSON if possible; if it's not JSON, we'll still hold raw bytes.
	var raw json.RawMessage
	if json.Valid(body) {
		raw = json.RawMessage(body)
	}

	// 2xx → success
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return ItemResult{
			ID:       id,
			OK:       true,
			Status:   resp.StatusCode,
			Received: raw,
		}
	}

	// Non-2xx → error; include body as string to help debugging
	return ItemResult{
		ID:       id,
		OK:       false,
		Status:   resp.StatusCode,
		Error:    string(body),
		Received: raw,
	}
}

// fetchWithRetry performs one fetch attempt and reports whether it should retry later.
// It does NOT sleep; it just returns the result and a bool flag telling caller to requeue.
func fetchWithRetry(ctx context.Context, client *http.Client, id, clientID string, maxRetries int) (ItemResult, bool) {
	var res ItemResult
	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		res = fetchOne(ctx, client, id, clientID)
		if res.OK {
			return res, false // success, don't retry
		}

		// Check if this type of error is worth retrying
		retryable := (res.Status == http.StatusTooManyRequests ||
			(res.Status >= 500 && res.Status <= 599) ||
			res.Status == 0)

		if !retryable {
			return res, false // permanent failure
		}

		// If there are more attempts left, break and signal caller to retry
		if attempt < maxRetries+1 {
			return res, true
		}
	}
	return res, false
}

// -------------------------------
// Orchestration: limit concurrency to N (≤5), use cache, collect results
// -------------------------------

// func main() {
// 	flag.Parse()
// 	start := time.Now()
// 	defer func() {
// 		fmt.Printf("\n⏱️  Total elapsed time: %v\n", time.Since(start))
// 	}()

// 	if *flagMaxConc > 5 {
// 		fmt.Println("❌ max_concurrency must be ≤ 5 to avoid rate limits")
// 		os.Exit(1)
// 	}

// 	// Parse IDs and load cache
// 	ids, err := parseIDsFromFlags()
// 	must(err)
// 	cache, err := loadCache(*flagCache)
// 	must(err)

// 	// Build optimized HTTP client
// 	httpClient := &http.Client{
// 		Timeout: *flagHTTPTimeout,
// 		Transport: &http.Transport{
// 			MaxIdleConns:    100,
// 			MaxConnsPerHost: 5,
// 			IdleConnTimeout: 90 * time.Second,
// 		},
// 	}

// 	// Figure out which IDs we still need to fetch
// 	toFetch := make([]string, 0, len(ids))
// 	for _, id := range ids {
// 		if prev, ok := cache[id]; ok && prev.OK {
// 			continue // skip cached success
// 		}
// 		toFetch = append(toFetch, id)
// 	}

// 	fmt.Printf("📊 Total IDs: %d | Cached OK: %d | To fetch: %d\n",
// 		len(ids), len(ids)-len(toFetch), len(toFetch))

// 	// If everything is already cached
// 	if len(toFetch) == 0 {
// 		fmt.Println("✅ Nothing new to fetch.")
// 		writeResults(*flagOut, flattenResults(ids, cache))
// 		return
// 	}

// 	// --- Set up concurrency infrastructure ---
// 	jobQueue := make(chan string, len(toFetch))
// 	results := make(chan ItemResult, len(toFetch))
// 	ctx := context.Background()
// 	var mu sync.Mutex
// 	var active int64

// 	// Fill initial job queue
// 	for _, id := range toFetch {
// 		jobQueue <- id
// 	}

// 	// Launch worker pool (up to 5)
// 	var wg sync.WaitGroup
// 	for i := 0; i < *flagMaxConc; i++ {
// 		wg.Add(1)
// 		go func(workerID int) {
// 			defer wg.Done()
// 			for id := range jobQueue {
// 				// Debugging Info: see whether all 5 workers are busy at once (for tuning speed)
// 				atomic.AddInt64(&active, 1)
// 				fmt.Printf("→ starting %s (active=%d)\n", id, atomic.LoadInt64(&active))

// 				// Each request gets its own timeout
// 				reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
// 				res, retry := fetchWithRetry(reqCtx, httpClient, id, *flagClientID, *flagMaxRetries)
// 				_ = res
// 				_ = retry
// 				cancel()

// 				atomic.AddInt64(&active, -1)

// 				mu.Lock()
// 				cache[id] = res
// 				mu.Unlock()

// 				results <- res

// 				if res.OK {
// 					fmt.Printf("✅ [%d] %s (status=%d)\n", workerID, id, res.Status)
// 				} else {
// 					fmt.Printf("⚠️ [%d] %s (status=%d) err=%s\n", workerID, id, res.Status, res.Error)
// 					if retry {
// 						// Delayed requeue for retry
// 						go func(id string) {
// 							delay := 2*time.Second + time.Duration(rand.Intn(1000))*time.Millisecond
// 							time.Sleep(delay)
// 							jobQueue <- id
// 						}(id)
// 					}
// 				}
// 			}
// 		}(i + 1)
// 	}


// 	// --- Collector ---
// 	go func() {
// 		wg.Wait()
// 		close(results)
// 	}()

// 	// --- Cache autosave ticker ---
// 	go func() {
// 		ticker := time.NewTicker(10 * time.Second)
// 		defer ticker.Stop()
// 		for range ticker.C {
// 			mu.Lock()
// 			saveCache(*flagCache, cache)
// 			mu.Unlock()
// 		}
// 	}()

// 	// --- Wait for all results ---
// 	completed := 0
// 	for r := range results {
// 		_ = r
// 		completed++
// 		if completed%10 == 0 {
// 			fmt.Printf("📦 Progress: %d/%d fetched\n", completed, len(ids))
// 		}
// 	}

// 	// Save final cache and results
// 	mu.Lock()
// 	must(saveCache(*flagCache, cache))
// 	mu.Unlock()
// 	must(writeResults(*flagOut, flattenResults(ids, cache)))

// 	fmt.Printf("💾 Saved cache to %s\n", *flagCache)
// 	fmt.Printf("📝 Wrote results to %s\n", *flagOut)
// }

// // flattenResults preserves the order of input IDs when writing results.
// func flattenResults(ids []string, cache CacheFile) []ItemResult {
// 	out := make([]ItemResult, 0, len(ids))
// 	for _, id := range ids {
// 		if r, ok := cache[id]; ok {
// 			out = append(out, r)
// 		} else {
// 			out = append(out, ItemResult{ID: id, OK: false, Status: 0, Error: "missing"})
// 		}
// 	}
// 	return out
// }
func main() {
	flag.Parse()
	start := time.Now()
	defer func() {
		fmt.Printf("\n⏱️  Total elapsed time: %v\n", time.Since(start))
	}()

	if *flagMaxConc > 5 {
		fmt.Println("❌ max_concurrency must be ≤ 5 to avoid rate limits")
		os.Exit(1)
	}

	// Parse IDs and load cache
	ids, err := parseIDsFromFlags()
	must(err)
	cache, err := loadCache(*flagCache)
	must(err)

	// Build optimized HTTP client
	httpClient := &http.Client{
		Timeout: *flagHTTPTimeout,
		Transport: &http.Transport{
			MaxIdleConns:    100,
			MaxConnsPerHost: 5,
			IdleConnTimeout: 90 * time.Second,
		},
	}

	// Determine which IDs to fetch (skip cached OK)
	toFetch := make([]string, 0, len(ids))
	for _, id := range ids {
		if prev, ok := cache[id]; ok && prev.OK {
			continue
		}
		toFetch = append(toFetch, id)
	}

	fmt.Printf("📊 Total IDs: %d | Cached OK: %d | To fetch: %d\n",
		len(ids), len(ids)-len(toFetch), len(toFetch))

	// If nothing new to fetch
	if len(toFetch) == 0 {
		fmt.Println("✅ Nothing new to fetch.")
		writeResults(*flagOut, flattenResults(ids, cache))
		return
	}

	// --- Setup concurrency infrastructure ---
	jobQueue := make(chan string, len(toFetch))
	results := make(chan ItemResult, len(toFetch))
	ctx := context.Background()
	var mu sync.Mutex
	var active, pending int64 // pending tracks total unprocessed jobs

	// Fill initial job queue
	for _, id := range toFetch {
		atomic.AddInt64(&pending, 1)
		jobQueue <- id
	}

	// --- Watcher: closes jobQueue when all jobs (including retries) are done ---
	go func() {
		for {
			time.Sleep(2 * time.Second)
			if atomic.LoadInt64(&pending) == 0 {
				close(jobQueue)
				return
			}
		}
	}()

	// --- Launch worker pool (≤5 concurrent requests) ---
	var wg sync.WaitGroup
	for i := 0; i < *flagMaxConc; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for id := range jobQueue {
				atomic.AddInt64(&active, 1)
				fmt.Printf("→ starting %s (active=%d)\n", id, atomic.LoadInt64(&active))

				reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				res, retry := fetchWithRetry(reqCtx, httpClient, id, *flagClientID, *flagMaxRetries)
				cancel()

				atomic.AddInt64(&active, -1)
				atomic.AddInt64(&pending, -1)

				mu.Lock()
				cache[id] = res
				mu.Unlock()

				results <- res

				if res.OK {
					fmt.Printf("✅ [%d] %s (status=%d)\n", workerID, id, res.Status)
				} else {
					fmt.Printf("⚠️ [%d] %s (status=%d) err=%s\n", workerID, id, res.Status, res.Error)
					if retry {
						// Retry with delay, but safely increment pending count
						go func(id string) {
							delay := 2*time.Second + time.Duration(rand.Intn(1000))*time.Millisecond
							time.Sleep(delay)
							atomic.AddInt64(&pending, 1)
							jobQueue <- id
						}(id)
					}
				}
			}
		}(i + 1)
	}

	// --- Collector: waits for workers to finish, then closes results ---
	go func() {
		wg.Wait()
		close(results)
	}()

	// --- Periodic cache autosave ---
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			mu.Lock()
			saveCache(*flagCache, cache)
			mu.Unlock()
		}
	}()

	// --- Consume results and track progress ---
	completed := 0
	for r := range results {
		_ = r
		completed++
		if completed%10 == 0 {
			fmt.Printf("📦 Progress: %d/%d fetched\n", completed, len(ids))
		}
	}

	// --- Final save ---
	mu.Lock()
	must(saveCache(*flagCache, cache))
	mu.Unlock()
	must(writeResults(*flagOut, flattenResults(ids, cache)))

	fmt.Printf("💾 Saved cache to %s\n", *flagCache)
	fmt.Printf("📝 Wrote results to %s\n", *flagOut)
}

// flattenResults preserves the order of input IDs when writing results.
func flattenResults(ids []string, cache CacheFile) []ItemResult {
	out := make([]ItemResult, 0, len(ids))
	for _, id := range ids {
		if r, ok := cache[id]; ok {
			out = append(out, r)
		} else {
			out = append(out, ItemResult{ID: id, OK: false, Status: 0, Error: "missing"})
		}
	}
	return out
}
