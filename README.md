# eluvio_app_challenge
Eluvio App challenge (open for feedback from community :) )
# Eluvio Applications Coding Challenge – Batch Item Fetcher

## 📘 Overview

This repository contains my submission for **Option 3 – Applications** in the Eluvio Software Engineer (New Grad) Challenge.

The goal is to implement a **client utility** that retrieves information for many item IDs as quickly as possible from a single-item API, while **respecting concurrency limits** and **avoiding unnecessary repeated queries**.

---

## 🧩 Problem Description

The API only supports **one ID per request** and allows a maximum of **five simultaneous requests**.  
Any additional requests trigger a **HTTP 429 (Too Many Requests)** error, causing a temporary 30 second rejection period.

**Task:**  
Write a program that:
1. Fetches item data efficiently for a large list of IDs.  
2. Avoids triggering rate limits (≤ 5 concurrent requests).  
3. Caches successful results locally to skip re-fetching already seen IDs.  
4. Retries transient failures (timeouts, 429s, and 5xx codes) with small backoff delays.

---

## ⚙️ How It Works

### Core Design

- **Worker Pool with Job Queue**  
  Up to 5 workers run in parallel, each reading from a shared queue of item IDs to process.

- **Atomic “Pending” Counter & Watcher**  
  The program tracks how many jobs are in flight (including retries).  
  When all jobs finish, it automatically closes the queue and exits cleanly.

- **Retry with Backoff**  
  Each failed request (timeout, 429, or 5xx) is retried after a random 2–3 s delay.

- **Persistent Cache**  
  Results are saved to `cache.json`, allowing later runs to skip already-successful IDs.  
  The full ordered results list is written to `results.json`.

- **Progress + Autosave**  
  Live progress is printed every 10 items, and cache autosaves run every 10 seconds.

---

## 🚀 Running the Program

### Prerequisites
- [Go 1.21+](https://go.dev/dl/)
- Internet access (to query the API)

### Example Command

```bash
go run main.go -client_id=test
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
- ✅ `cache.json` keeps all previously fetched items.
- ✅ `results.json` lists ordered responses for all IDs.

---

## 🧠 Notes on Design Choices

- Uses `context.WithTimeout` per request to prevent stuck connections.  
- Enforces `max_concurrency ≤ 5` for safety against 429s.  
- Retries transient errors with randomized delay to avoid synchronized retry spikes.  
- Clean shutdown using an atomic pending counter — no panic, no hang.

---

## 🧑‍💻 Author

**Truc Tran**  
B.A. Computer Science @ UC Berkeley  
Eluvio New Grad Challenge Fall 2025 Submission – Option 3 (Applications)
