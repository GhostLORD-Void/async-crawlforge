# Async CrawlForge

A lightweight asynchronous, same-host web crawler with a FastAPI control API, SQLite storage, full-text search, and JSON/CSV export.

## Features

- Asynchronous HTTP fetching with bounded concurrency and request timeouts
- Same-host crawling with configurable page and depth limits
- Redirect validation and blocking of non-public IP destinations
- HTML title, meta description, text, and link extraction
- SQLite persistence and FTS5 full-text search
- Background crawl jobs with status endpoint
- JSON and CSV export

## Requirements

- Python 3.11+
- SQLite built with FTS5 support (included in most standard Python distributions)

## Run locally

```bash
python -m venv .venv
# macOS/Linux
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1

pip install -r requirements.txt
uvicorn app:app --reload --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000/docs for the interactive API documentation.

## API

Start a crawl:

```bash
curl -X POST http://127.0.0.1:8000/crawl \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com","max_pages":20,"max_depth":2,"delay_seconds":0.5}'
```

Check the returned job:

```bash
curl http://127.0.0.1:8000/jobs/JOB_ID
```

Other endpoints:

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Health check |
| POST | `/crawl` | Start a crawl |
| GET | `/jobs/{job_id}` | Check job progress |
| GET | `/search?q=words` | Search indexed pages |
| GET | `/stats` | Database counts |
| GET | `/export/json` | Download JSON |
| GET | `/export/csv` | Download CSV |

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `CRAWL_DB_PATH` | `crawlforge.db` | SQLite database path |
| `REQUEST_TIMEOUT` | `15` | HTTP request timeout in seconds |
| `CRAWL_API_KEY` | unset | If set, protected API endpoints require `X-API-Key` |

Example:

```bash
export CRAWL_API_KEY="replace-with-a-long-random-secret"
uvicorn app:app --host 127.0.0.1 --port 8000
```

## Security and operational notes

- Keep the service bound to localhost unless you have placed it behind authentication, TLS, rate limiting, and network-level egress controls.
- Set `CRAWL_API_KEY` before exposing the API. The key is an application-level gate, not a substitute for a proper reverse proxy or identity provider.
- The crawler restricts page navigation to the starting hostname, checks DNS answers for non-public IPs, disables environment proxy inheritance, and validates redirects. DNS rebinding and infrastructure-specific network paths still require network egress controls for hostile-input deployments.
- Crawl only websites you are authorized to access. Respect site terms, robots policies, and rate limits.
- Job state is held in process memory. Restarting the server loses job status; use a durable queue for multi-worker or production deployments.
- SQLite is intended for a single application instance. Use a managed database and a durable task queue for horizontal scaling.

## License

MIT
