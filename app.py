from __future__ import annotations

import asyncio
import csv
from contextlib import contextmanager
import io
import ipaddress
import json
import os
import socket
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

APP_NAME = "Async CrawlForge"
DB_PATH = os.getenv("CRAWL_DB_PATH", "crawlforge.db")
MAX_PAGES_HARD_LIMIT = 500
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15"))
USER_AGENT = "AsyncCrawlForge/1.0 (+https://github.com/GhostLORD-Void/async-crawlforge)"
API_KEY = os.getenv("CRAWL_API_KEY")

app = FastAPI(title=APP_NAME, version="1.0.0", description="A bounded, same-host asynchronous web crawler.")
jobs: dict[str, dict[str, Any]] = {}
job_tasks: dict[str, asyncio.Task] = {}
db_lock = asyncio.Lock()


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def db_session():
    conn = db_connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with db_session() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                final_url TEXT NOT NULL,
                status_code INTEGER,
                title TEXT,
                description TEXT,
                content TEXT,
                crawled_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS links (
                source_url TEXT NOT NULL,
                target_url TEXT NOT NULL,
                PRIMARY KEY (source_url, target_url)
            )
        """)
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS page_search
            USING fts5(url UNINDEXED, title, content)
        """)


@app.on_event("startup")
async def startup() -> None:
    init_db()


async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="A valid X-API-Key header is required.")


def normalize_url(raw_url: str) -> str:
    raw_url = raw_url.strip()
    parsed = urlparse(raw_url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Only absolute HTTP and HTTPS URLs are allowed.")
    if parsed.username or parsed.password:
        raise ValueError("URLs containing credentials are not allowed.")
    host = parsed.hostname.lower().rstrip(".")
    port = parsed.port
    netloc = host if port is None else f"{host}:{port}"
    path = parsed.path or "/"
    normalized = urlunparse((parsed.scheme.lower(), netloc, path, "", parsed.query, ""))
    return urldefrag(normalized)[0]


async def validate_public_host(url: str, expected_host: str) -> None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    if host != expected_host:
        raise ValueError("Cross-host navigation is not allowed.")
    if parsed.port not in (None, 80, 443):
        raise ValueError("Only standard HTTP/HTTPS ports are allowed.")
    try:
        records = await asyncio.to_thread(
            socket.getaddrinfo, host, parsed.port or (443 if parsed.scheme == "https" else 80),
            type=socket.SOCK_STREAM
        )
    except socket.gaierror as exc:
        raise ValueError("Hostname could not be resolved.") from exc
    addresses = {record[4][0] for record in records}
    if not addresses:
        raise ValueError("Hostname has no address records.")
    for address in addresses:
        ip = ipaddress.ip_address(address.split("%")[0])
        if not ip.is_global:
            raise ValueError("Private, loopback, link-local, and reserved network targets are blocked.")


class CrawlRequest(BaseModel):
    url: str = Field(..., description="Starting HTTP/HTTPS URL")
    max_pages: int = Field(default=25, ge=1, le=MAX_PAGES_HARD_LIMIT)
    max_depth: int = Field(default=2, ge=0, le=10)
    delay_seconds: float = Field(default=0.5, ge=0, le=10)


async def save_page(url: str, final_url: str, status: int, title: str,
                    description: str, content: str, links: list[str]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    async with db_lock:
        with db_session() as conn:
            conn.execute("""
                INSERT INTO pages(url, final_url, status_code, title, description, content, crawled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    final_url=excluded.final_url, status_code=excluded.status_code,
                    title=excluded.title, description=excluded.description,
                    content=excluded.content, crawled_at=excluded.crawled_at
            """, (url, final_url, status, title, description, content, now))
            conn.execute("DELETE FROM page_search WHERE url = ?", (url,))
            conn.execute("INSERT INTO page_search(url, title, content) VALUES (?, ?, ?)",
                         (url, title, content))
            conn.executemany(
                "INSERT OR IGNORE INTO links(source_url, target_url) VALUES (?, ?)",
                [(url, target) for target in links]
            )


async def fetch_page(client: httpx.AsyncClient, url: str, host: str):
    current = url
    for _ in range(4):
        await validate_public_host(current, host)
        response = await client.get(current, follow_redirects=False)
        if response.status_code in {301, 302, 303, 307, 308}:
            location = response.headers.get("location")
            if not location:
                break
            current = normalize_url(urljoin(current, location))
            continue
        return response, current
    raise ValueError("Too many redirects or redirect without a location.")


async def run_crawl(job_id: str, request: CrawlRequest) -> None:
    start_url = normalize_url(request.url)
    host = (urlparse(start_url).hostname or "").lower().rstrip(".")
    queue: list[tuple[str, int]] = [(start_url, 0)]
    visited: set[str] = set()
    counts = {"crawled": 0, "failed": 0, "discovered": 1}
    jobs[job_id]["status"] = "running"

    limits = httpx.Limits(max_connections=5, max_keepalive_connections=2)
    timeout = httpx.Timeout(REQUEST_TIMEOUT)
    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"},
        timeout=timeout, limits=limits, trust_env=False
    ) as client:
        while queue and len(visited) < request.max_pages:
            url, depth = queue.pop(0)
            if url in visited:
                continue
            visited.add(url)
            try:
                response, final_url = await fetch_page(client, url, host)
                content_type = response.headers.get("content-type", "").lower()
                if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                    counts["failed"] += 1
                    continue

                soup = BeautifulSoup(response.text[:2_000_000], "html.parser")
                title = soup.title.get_text(" ", strip=True) if soup.title else ""
                meta = soup.find("meta", attrs={"name": "description"})
                description = meta.get("content", "").strip() if meta else ""
                for node in soup(["script", "style", "noscript", "svg"]):
                    node.decompose()
                text_content = soup.get_text(" ", strip=True)[:100_000]

                found: list[str] = []
                if depth < request.max_depth:
                    for anchor in soup.find_all("a", href=True):
                        try:
                            target = normalize_url(urljoin(final_url, anchor["href"]))
                            if (urlparse(target).hostname or "").lower().rstrip(".") == host:
                                if target not in visited and all(target != item[0] for item in queue):
                                    found.append(target)
                        except (ValueError, TypeError):
                            continue
                await save_page(url, final_url, response.status_code, title,
                                description, text_content, found)
                counts["crawled"] += 1
                for target in found:
                    if len(visited) + len(queue) < request.max_pages:
                        queue.append((target, depth + 1))
                counts["discovered"] = len(visited) + len(queue)
            except (httpx.HTTPError, ValueError, UnicodeError):
                counts["failed"] += 1
            jobs[job_id].update(counts)
            if request.delay_seconds:
                await asyncio.sleep(request.delay_seconds)

    jobs[job_id].update(counts)
    jobs[job_id]["status"] = "completed"
    jobs[job_id]["finished_at"] = datetime.now(timezone.utc).isoformat()


@app.get("/")
async def root():
    return {"name": APP_NAME, "version": "1.0.0", "docs": "/docs", "health": "/health"}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/crawl", dependencies=[Depends(require_api_key)])
async def start_crawl(request: CrawlRequest):
    try:
        start_url = normalize_url(request.url)
        await validate_public_host(start_url, (urlparse(start_url).hostname or "").lower().rstrip("."))
    except (ValueError, httpx.HTTPError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "job_id": job_id, "status": "queued", "url": start_url,
        "max_pages": request.max_pages, "max_depth": request.max_depth,
        "crawled": 0, "failed": 0, "discovered": 1,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    task = asyncio.create_task(run_crawl(job_id, request))
    job_tasks[job_id] = task
    return jobs[job_id]


@app.get("/jobs/{job_id}", dependencies=[Depends(require_api_key)])
async def get_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found. Job state is kept in memory.")
    return job


@app.get("/search", dependencies=[Depends(require_api_key)])
async def search_pages(q: str = Query(..., min_length=2, max_length=200)):
    try:
        with db_session() as conn:
            rows = conn.execute("""
                SELECT url, title, snippet(page_search, 2, '<b>', '</b>', '…', 16) AS snippet
                FROM page_search WHERE page_search MATCH ? LIMIT 50
            """, (q,)).fetchall()
        return {"query": q, "count": len(rows), "results": [dict(row) for row in rows]}
    except sqlite3.OperationalError as exc:
        raise HTTPException(status_code=400, detail="Invalid search syntax or search query.") from exc


@app.get("/stats", dependencies=[Depends(require_api_key)])
async def stats():
    with db_session() as conn:
        pages = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
        links = conn.execute("SELECT COUNT(*) FROM links").fetchone()[0]
    return {"pages": pages, "links": links, "active_jobs": sum(
        1 for job in jobs.values() if job["status"] in {"queued", "running"}
    )}


@app.get("/export/{format}", dependencies=[Depends(require_api_key)])
async def export_pages(format: str):
    if format not in {"json", "csv"}:
        raise HTTPException(status_code=400, detail="Format must be json or csv.")
    with db_session() as conn:
        rows = [dict(row) for row in conn.execute(
            "SELECT url, final_url, status_code, title, description, content, crawled_at FROM pages ORDER BY crawled_at DESC"
        ).fetchall()]
    if format == "json":
        payload = json.dumps(rows, ensure_ascii=False, indent=2)
        return StreamingResponse(io.BytesIO(payload.encode()), media_type="application/json",
                                 headers={"Content-Disposition": "attachment; filename=pages.json"})
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()) if rows else
                            ["url", "final_url", "status_code", "title", "description", "content", "crawled_at"])
    writer.writeheader()
    writer.writerows(rows)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=pages.csv"})
