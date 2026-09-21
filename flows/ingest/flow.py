from metaflow import (
    step,
    retry,
    resources,
    kubernetes,
    pypi,
    card,
    current,
    Parameter,
    S3,
)
from metaflow.cards import Markdown
from obproject import ProjectFlow

BASE_URL = "https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/EDGES/"

# Only the columns the dashboard reads. TIGER EDGES ships ~30; the rest are
# address ranges and feature codes nothing downstream touches. Read at the
# shapefile, so the other columns are never parsed or carried anywhere.
KEEP_COLUMNS = ["STATEFP", "COUNTYFP", "FULLNAME"]

# Parquet's smallest readable unit is a row group's column chunk, so row-group
# size sets the floor on how little a lookup can fetch. Measured on run 34:
# 1M-row groups put 110 MB of geometry in each chunk, so pulling 95k rows of
# "Main St" downloaded ~110 MB to use ~6 MB of it (13s over the network).
# 100k rows puts that chunk near 11 MB. The cost is more footer metadata and
# slightly worse compression.
ROW_GROUP_SIZE = 100_000

# The counts table is looked up the same way, and it is small, so it gets
# small row groups too.
COUNTS_ROW_GROUP_SIZE = 50_000


class MapnStreetsFlow(ProjectFlow):

    # Batching trades pod-startup overhead against retry granularity: a 429 on
    # the last url of a batch re-downloads the whole batch. Parameterized so
    # the tradeoff can be measured rather than guessed.
    chunk_size = Parameter(
        "chunk-size", default=20, type=int, help="urls per task"
    )
    url_limit = Parameter(
        "url-limit", default=0, type=int,
        help="cap the url list for experiments (0 = all)",
    )
    # In-task pacing. Note this is NOT the global request rate: tasks run
    # concurrently, so the real rate is roughly max_workers / delay. Use
    # --max-workers as the politeness knob and leave this at 0.
    request_delay = Parameter(
        "request-delay", default=0.0, type=float,
        help="seconds between requests within a task",
    )

    @pypi(
        packages={
            "requests": "2.32.3",
            "beautifulsoup4": "4.15.0",
        }
    )
    @step
    def start(self):
        import time
        import requests
        from bs4 import BeautifulSoup

        print(f"[start] fetching partition listing from {BASE_URL}")
        t0 = time.time()
        resp = requests.get(BASE_URL, timeout=30)
        resp.raise_for_status()
        print(f"[start] listing fetched in {time.time() - t0:.1f}s, status={resp.status_code}")

        soup = BeautifulSoup(resp.text, "html.parser")
        urls = [
            BASE_URL + a.get("href")
            for a in soup.find_all("a")
            if a.get("href", "").endswith(".zip")
        ]
        print(f"[start] found {len(urls)} shapefile URLs")
        if not urls:
            raise RuntimeError(
                "[start] no URLs found — check page structure / BASE_URL. "
                "A foreach over an empty list is a hard failure, not a no-op."
            )

        if self.url_limit:
            urls = urls[: self.url_limit]
            print(f"[start] capped to {len(urls)} urls (--url-limit)")

        self.url_batches = [
            urls[i : i + self.chunk_size]
            for i in range(0, len(urls), self.chunk_size)
        ]
        print(
            f"[start] {len(self.url_batches)} batches of up to "
            f"{self.chunk_size} urls"
        )

        self.next(self.download_and_process, foreach="url_batches")

    @retry(times=4, minutes_between_retries=1)
    @resources(memory=8000)
    @kubernetes(cpu=2, memory=8000, disk=20000)
    @pypi(
        python="3.11",
        packages={
            "geopandas": "1.1.3",
            "shapely": "2.1.1",
            "pandas": "3.0.5",
            "pyarrow": "",
            "requests": "2.32.3",
            "beautifulsoup4": "4.15.0",
            "boto3": "",
        }
    )
    @step
    def download_and_process(self):
        """Download a batch, write it as a pruned WKB parquet part, upload it.

        The conversion happens here rather than in the join for two reasons:
        these tasks are already parallel, and it means the batch never becomes
        a ~500 MB pickled DataFrame artifact. Only the part's key crosses the
        join, so nothing large meets at a single task.
        """
        import time
        import requests
        import geopandas as gpd
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        import shapely
        from pathlib import Path

        batch = self.input
        t_task = time.time()
        print(f"[download] batch started, {len(batch)} urls")

        frames = []
        t_net = 0.0
        for i, url in enumerate(batch):
            fname = Path(url).name
            local_zip = Path("/tmp") / fname

            t0 = time.time()
            for attempt in range(5):
                r = requests.get(url, timeout=60)
                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", 5))
                    print(f"[download] 429 on {fname}, waiting {wait}s (Retry-After)")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                break
            else:
                raise RuntimeError(f"still rate-limited on {fname} after 5 attempts")

            local_zip.write_bytes(r.content)
            t_net += time.time() - t0

            # columns= pushes the projection into the shapefile reader, so the
            # ~26 unused columns are never parsed.
            gdf = gpd.read_file(f"zip://{local_zip}", columns=KEEP_COLUMNS)
            frames.append(gdf)
            local_zip.unlink()

            print(
                f"[download] {fname}: {len(gdf)} rows "
                f"in {time.time() - t0:.1f}s"
            )

            if self.request_delay and i < len(batch) - 1:
                time.sleep(self.request_delay)

        gdf = pd.concat(frames, ignore_index=True)
        del frames

        self.part_key = None
        self.rows = 0
        self.urls_done = len(batch)
        self.net_seconds = t_net
        if len(gdf) == 0:
            print("[download] batch is empty, no part written")
            self.task_seconds = time.time() - t_task
            self.next(self.join)
            return

        # WKB binary rather than WKT text: 2-3x smaller, and the reader skips a
        # per-row text parse. The string cast normalizes dtypes so every part
        # shares one schema.
        table = pa.table(
            {
                **{
                    c: pa.array(gdf[c].astype("string[python]"))
                    for c in KEEP_COLUMNS
                },
                "geometry": pa.array(
                    shapely.to_wkb(gdf.geometry.to_numpy()), type=pa.binary()
                ),
            }
        )
        del gdf

        local = Path("/tmp") / f"part-{current.task_id}.parquet"
        pq.write_table(table, local, compression="zstd")

        key = f"parts/part-{current.task_id}.parquet"
        with S3(run=self) as s3:
            s3.put_files([(key, str(local))])

        self.part_key = key
        self.rows = table.num_rows
        self.task_seconds = time.time() - t_task
        print(
            f"[download] wrote {table.num_rows} rows -> "
            f"{local.stat().st_size / 1e6:.1f} MB, uploaded {key}"
        )
        print(
            f"[download] task {self.task_seconds:.1f}s "
            f"({t_net:.1f}s network, {len(batch)} urls)"
        )
        self.next(self.join)

    @kubernetes(cpu=4, memory=12000, disk=80000)
    @pypi(
        python="3.11",
        packages={
            "pandas": "3.0.5",
            "pyarrow": "",
            "duckdb": "",
            "boto3": "",
        },
    )
    @card
    @step
    def join(self, inputs):
        import time
        from pathlib import Path

        import duckdb

        # Only keys cross the join; the parts themselves are in S3.
        keys = []
        total_rows = 0
        empty_batches = 0
        task_times = []
        net_times = []
        urls_done = 0
        for inp in inputs:
            task_times.append(inp.task_seconds)
            net_times.append(inp.net_seconds)
            urls_done += inp.urls_done
            if inp.part_key is None:
                empty_batches += 1
            else:
                keys.append(inp.part_key)
                total_rows += inp.rows

        # Compute time vs wall clock is the batching tradeoff: the gap is pod
        # startup, and it is what batching amortizes.
        n_tasks = len(task_times)
        print(
            f"[join] {n_tasks} tasks, {urls_done} urls, "
            f"{sum(task_times):.0f}s total task time "
            f"({sum(task_times) / n_tasks:.1f}s avg, "
            f"max {max(task_times):.1f}s), "
            f"{sum(net_times):.0f}s of that network"
        )
        print(f"[join] {len(keys)} parts, {total_rows} rows, {empty_batches} empty")
        if not keys:
            raise RuntimeError("[join] every batch was empty, nothing to write")

        work = Path("/tmp/mapnstreets")
        parts = work / "parts"
        parts.mkdir(parents=True, exist_ok=True)
        (work / "spill").mkdir(exist_ok=True)

        # get_many fetches in parallel with retries.
        print(f"[join] fetching {len(keys)} parts", flush=True)
        t0 = time.time()
        with S3(run=self) as s3:
            for obj in s3.get_many(keys):
                # obj.path is cleaned up when the S3 block exits, so move it out.
                Path(obj.path).replace(parts / Path(obj.key).name)
        parts_mb = sum(f.stat().st_size for f in parts.glob("*.parquet")) / 1e6
        print(f"[join] fetched {parts_mb:.1f} MB in {time.time() - t0:.1f}s")

        # Sort globally on FULLNAME so a reader can prune row groups instead of
        # scanning: unsorted, every group spans A-Z and an exact-match lookup
        # has to read all of them. DuckDB spills the sort to disk, so this
        # needs temp space rather than RAM.
        edges = work / "edges.parquet"
        counts = work / "name_counts.parquet"
        print("[join] sorting on FULLNAME", flush=True)
        t1 = time.time()

        con = duckdb.connect()
        con.execute("SET memory_limit='8GB'")
        con.execute(f"SET temp_directory='{work / 'spill'}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute(
            f"""
            COPY (
                SELECT {', '.join(KEEP_COLUMNS)}, geometry
                FROM read_parquet('{parts}/*.parquet')
                WHERE FULLNAME IS NOT NULL
                ORDER BY FULLNAME
            ) TO '{edges}' (
                FORMAT parquet, COMPRESSION zstd,
                ROW_GROUP_SIZE {ROW_GROUP_SIZE}
            )
            """
        )

        # name -> count, so the app's record count never touches the big file.
        con.execute(
            f"""
            COPY (
                SELECT FULLNAME, count(*) AS n
                FROM read_parquet('{edges}')
                GROUP BY FULLNAME
                ORDER BY FULLNAME
            ) TO '{counts}' (
                FORMAT parquet, COMPRESSION zstd,
                ROW_GROUP_SIZE {COUNTS_ROW_GROUP_SIZE}
            )
            """
        )
        distinct_names = con.execute(
            f"SELECT count(*) FROM read_parquet('{counts}')"
        ).fetchone()[0]

        # Report the layout that determines query cost, so it is on the run
        # rather than something to re-derive with a probe script.
        geom_mb, n_groups = con.execute(
            f"""
            SELECT avg(total_compressed_size) / 1e6, count(*)
            FROM parquet_metadata('{edges}')
            WHERE path_in_schema = 'geometry'
            """
        ).fetchone()
        con.close()

        edges_mb = edges.stat().st_size / 1e6
        counts_mb = counts.stat().st_size / 1e6
        print(
            f"[join] sorted in {time.time() - t1:.1f}s: {edges_mb:.1f} MB, "
            f"{n_groups} row groups, {geom_mb:.1f} MB geometry per group"
        )

        with S3(run=self) as s3:
            uploaded = s3.put_files(
                [
                    ("edges.parquet", str(edges)),
                    ("name_counts.parquet", str(counts)),
                ]
            )
        urls = dict(uploaded)
        self.edges_url = urls["edges.parquet"]
        self.counts_url = urls["name_counts.parquet"]
        self.total_rows = total_rows
        self.distinct_names = distinct_names
        print(f"[join] uploaded {self.edges_url}")
        print(f"[join] uploaded {self.counts_url}")

        current.card.append(Markdown(f"# Processed {len(keys)} batches"))
        current.card.append(
            Markdown(
                f"**Batching:** {n_tasks} tasks for {urls_done} urls "
                f"(chunk-size {self.chunk_size}), "
                f"{sum(task_times):.0f}s total task time, "
                f"{sum(net_times):.0f}s network"
            )
        )
        current.card.append(Markdown(f"Total rows: {total_rows}"))
        current.card.append(Markdown(f"Distinct names: {distinct_names}"))
        current.card.append(Markdown(f"Empty batches: {empty_batches}"))
        current.card.append(
            Markdown(f"Sorted parquet: {edges_mb:.1f} MB (parts were {parts_mb:.1f} MB)")
        )
        current.card.append(
            Markdown(
                f"Layout: {n_groups} row groups, "
                f"{geom_mb:.1f} MB geometry per group — this is what a "
                "single-name lookup has to fetch"
            )
        )
        current.card.append(Markdown(f"Counts table: {counts_mb:.1f} MB"))
        current.card.append(Markdown(f"Location: `{self.edges_url}`"))
        self.next(self.end)

    @step
    def end(self):
        print(
            f"[end] {self.total_rows} rows, {self.distinct_names} distinct names"
        )
        print(f"[end] edges  {self.edges_url}")
        print(f"[end] counts {self.counts_url}")


if __name__ == "__main__":
    MapnStreetsFlow()
