# Eluvio Software Engineer Challenge Fall 2025 – Option 3: Applications

### Problem Overview

Imagine you have a program that needs to look up information about items using their item ID, often in large batches.

Unfortunately, the only API available for returning this data takes **one item at a time**, and is limited to **5 simultaneous requests**.  
Any additional requests beyond that limit are rejected for 30 seconds with **HTTP 429**.

**Goal:**  
Write a client utility that retrieves information for all given item IDs **as quickly as possible** without:
- Exceeding the concurrency limit,
- Triggering rate-limit lockouts,
- Or re-fetching already-seen items (via caching).

---

## Approach / Strategy

This solution is implemented in **Go (Golang)** for its efficient concurrency primitives and simple HTTP client handling.

Key design ideas:
1. **Worker Pool with Bounded Concurrency**  
   - Uses a queue (`chan string`) and a pool of ≤ 5 workers.  
   - Guarantees no more than 5 requests are active at once.

2. **Exponential Backoff Retries**  
   - Automatically retries transient errors (`429`, `5xx`, or timeouts).  
   - Wait time includes a small randomized delay to avoid synchronized retry bursts.

3. **Persistent Cache**  
   - Each item ID result is cached to `cache.json` to avoid redundant queries in future runs.

4. **Timeouts and Contexts**  
   - Each HTTP call runs under a `context.WithTimeout` (default 15 s) to prevent hanging goroutines.

5. **Graceful Shutdown**  
   - Tracks active workers atomically; closes result channels cleanly when all work is done.

6. **Progress Feedback**  
   - Prints per-request logs and progress counters for transparency.

---

## Running the Program

### Prerequisites
- [Go 1.21+](https://go.dev/dl/)
- Internet access (to query the API)

### Example Command

```bash
# In terminal, run:
go run main.go -client_id=test
```
```bash
# And add those output files to your `.gitignore`:
cache.json
results.json
```
### Optional Flags

| Flag | Description | Default |
|------|--------------|----------|
| `-ids` | Comma-separated list of item IDs | built-in 50 example IDs |
| `-ids_file` | Path to file containing one ID per line | none |
| `-client_id` | String identifier for your requests | `eluvio` |
| `-out` | Output results file | `results.json` |
| `-cache` | Persistent cache file | `cache.json` |
| `-max_concurrency` | Maximum concurrent requests (≤ 5) | `5` |
| `-max_retries` | Number of retry attempts per ID | `2` |
| `-http_timeout` | Per-request timeout | `15s` |


### Example Output

```bash
📊 Total IDs: 50 | Cached OK: 0 | To fetch: 50
→ starting b6589fc6 (active=1)
→ starting 356a192b (active=2)
...
📦 Progress: 50/50 fetched
💾 Saved cache to cache.json
📝 Wrote results to results.json
⏱️  Total elapsed time: 50.0s
```

After completion:
- `cache.json` keeps all previously fetched items.
- `results.json` lists ordered responses for all IDs.

---
## How to Run
```bash
# Clone the repo
git clone git@github.com:<your_username>/eluvio_app_challenge.git
cd eluvio_app_challenge

# (Optional) Initialize Go module if needed
go mod init eluvio_app_challenge
go mod tidy

# Run the program
go run main.go -client_id=test
```

---

## Notes on Design Choices

- Uses `context.WithTimeout` per request to prevent stuck connections.  
- Enforces `max_concurrency ≤ 5` for safety against 429s.  
- Retries transient errors with randomized delay to avoid synchronized retry spikes.  
- Clean shutdown using an atomic pending counter — no panic, no hang.

---

## Future Improvements

- Dynamic backoff tuned by response headers.
- CLI flag for custom retry delay profile.
- Metrics summary (average latency, retry count).

---

## Author

**Truc Tran**  
B.A. Computer Science @ UC Berkeley  
Eluvio New Grad Challenge Fall 2025 Submission – Option 3 (Applications)
