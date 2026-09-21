"""Quick sanity check: validate that discovered URLs return real, non-empty zip files.

Uses low concurrency + retry/backoff since census.gov rate-limits aggressive parallel
HEAD requests (HTTP 429).

Usage:
    python validate_urls.py
"""
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/EDGES/"

MAX_WORKERS = 6          # census.gov starts 429ing well before 32
MAX_RETRIES = 4
BASE_BACKOFF = 2.0       # seconds, doubles each retry


def get_urls():
    delay = BASE_BACKOFF
    for attempt in range(MAX_RETRIES):
        resp = requests.get(BASE_URL, timeout=30)
        if resp.status_code == 429:
            print(f"listing request rate-limited, waiting {delay:.0f}s before retry...")
            time.sleep(delay)
            delay *= 2
            continue
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        return [
            BASE_URL + a.get("href")
            for a in soup.find_all("a")
            if a.get("href", "").endswith(".zip")
        ]
    raise RuntimeError("listing endpoint still rate-limited after retries")


def check(url, timeout=15):
    delay = BASE_BACKOFF
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.head(url, timeout=timeout, allow_redirects=True)
            if r.status_code == 429:
                time.sleep(delay)
                delay *= 2
                continue
            ctype = r.headers.get("Content-Type", "")
            clen = int(r.headers.get("Content-Length", 0))
            ok = r.status_code == 200 and clen > 0
            return url, ok, r.status_code, ctype, clen
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return url, False, None, str(e), 0
            time.sleep(delay)
            delay *= 2
    return url, False, 429, "rate limited after retries", 0


def main():
    urls = get_urls()
    print(f"checking {len(urls)} urls with {MAX_WORKERS} workers...")

    bad = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(check, u) for u in urls]
        for i, fut in enumerate(as_completed(futures), 1):
            url, ok, status, ctype, clen = fut.result()
            if not ok:
                bad.append((url, status, ctype, clen))
            if i % 250 == 0:
                print(f"  checked {i}/{len(urls)}...")

    print(f"\n{len(urls) - len(bad)}/{len(urls)} URLs look valid")
    if bad:
        print(f"{len(bad)} problematic URLs:")
        for url, status, ctype, clen in bad[:50]:
            print(f"  status={status} ctype={ctype} len={clen}  {url}")
        if len(bad) > 50:
            print(f"  ... and {len(bad) - 50} more")


if __name__ == "__main__":
    main()
