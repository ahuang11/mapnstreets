"""Benchmark storage layouts for the MapnStreets edges dataset.

Builds several variants from the run's parquet and times the app's actual query
pattern (exact match on FULLNAME) against each.

    python bench.py --help
    python bench.py --build          # build variants (slow, minutes)
    python bench.py                  # time whatever variants exist

Zarr is deliberately absent: it stores dense N-dimensional arrays indexed by
position, and these are ragged LineStrings looked up by attribute value. Forcing
them into an array would benchmark a workload nobody would run.
"""

import argparse
import shutil
import tempfile
import time
from pathlib import Path

import duckdb

SOURCE = Path(tempfile.gettempdir()) / "mapnstreets_edges.parquet"
WORK = Path(tempfile.gettempdir()) / "mapnstreets_bench"

COLS = ["STATEFP", "COUNTYFP", "FULLNAME"]
MAX_DRAWN = 5000
REPEATS = 3

# ORDER BY over ~63M rows spills; give DuckDB room and somewhere to spill to.
SETUP = [
    "SET memory_limit='8GB'",
    f"SET temp_directory='{WORK / 'spill'}'",
    "SET preserve_insertion_order=false",
]


def connect():
    con = duckdb.connect()
    for stmt in SETUP:
        con.execute(stmt)
    return con


def has_spatial(con):
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        return True
    except Exception:
        return False


# --------------------------------------------------------------------------
# Variant builders. Each returns a path (file or directory).
# --------------------------------------------------------------------------


def build_pruned(con, out):
    """Only the columns the app reads. Geometry stays WKT."""
    con.execute(
        f"""
        COPY (
            SELECT {', '.join(COLS)}, geometry
            FROM read_parquet('{SOURCE}')
            WHERE FULLNAME IS NOT NULL
        ) TO '{out}' (FORMAT parquet, COMPRESSION zstd)
        """
    )


def build_wkb(con, out):
    """Pruned, with geometry as WKB instead of WKT."""
    con.execute(
        f"""
        COPY (
            SELECT {', '.join(COLS)},
                   ST_AsWKB(ST_GeomFromText(geometry)) AS geometry
            FROM read_parquet('{SOURCE}')
            WHERE FULLNAME IS NOT NULL
        ) TO '{out}' (FORMAT parquet, COMPRESSION zstd)
        """
    )


def build_sorted(con, out):
    """Pruned + sorted on the query column, so row-group stats can prune."""
    con.execute(
        f"""
        COPY (
            SELECT {', '.join(COLS)}, geometry
            FROM read_parquet('{SOURCE}')
            WHERE FULLNAME IS NOT NULL
            ORDER BY FULLNAME
        ) TO '{out}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)
        """
    )


def build_partitioned(con, out):
    """Hive-partitioned by state, sorted within each partition."""
    con.execute(
        f"""
        COPY (
            SELECT {', '.join(COLS)}, geometry
            FROM read_parquet('{SOURCE}')
            WHERE FULLNAME IS NOT NULL
            ORDER BY STATEFP, FULLNAME
        ) TO '{out}' (
            FORMAT parquet, COMPRESSION zstd,
            PARTITION_BY (STATEFP), OVERWRITE_OR_IGNORE
        )
        """
    )


def build_duckdb_indexed(con, out):
    """A DuckDB file with a real index on FULLNAME."""
    db = duckdb.connect(str(out))
    for stmt in SETUP:
        db.execute(stmt)
    db.execute(
        f"""
        CREATE TABLE edges AS
        SELECT {', '.join(COLS)}, geometry
        FROM read_parquet('{SOURCE}')
        WHERE FULLNAME IS NOT NULL
        """
    )
    db.execute("CREATE INDEX idx_fullname ON edges(FULLNAME)")
    db.close()


VARIANTS = {
    "baseline": (None, lambda: SOURCE),
    "pruned": (build_pruned, lambda: WORK / "pruned.parquet"),
    "wkb": (build_wkb, lambda: WORK / "wkb.parquet"),
    "sorted": (build_sorted, lambda: WORK / "sorted.parquet"),
    "partitioned": (build_partitioned, lambda: WORK / "partitioned"),
    "duckdb": (build_duckdb_indexed, lambda: WORK / "edges.duckdb"),
}


# --------------------------------------------------------------------------
# Measurement
# --------------------------------------------------------------------------


def size_bytes(path):
    if path.is_dir():
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    return path.stat().st_size


def pick_names(con):
    """Three names spanning the selectivity range, from the data itself."""
    rows = con.execute(
        f"""
        WITH counts AS (
            SELECT FULLNAME, count(*) AS n
            FROM read_parquet('{SOURCE}')
            WHERE FULLNAME IS NOT NULL
            GROUP BY FULLNAME
        ), ranked AS (
            SELECT FULLNAME, n,
                   row_number() OVER (ORDER BY n DESC) AS hi,
                   row_number() OVER (ORDER BY n ASC) AS lo,
                   count(*) OVER () AS total
            FROM counts
        )
        SELECT FULLNAME, n FROM ranked WHERE hi = 1
        UNION ALL
        SELECT FULLNAME, n FROM ranked WHERE hi = total / 2
        UNION ALL
        SELECT FULLNAME, n FROM ranked WHERE lo = 1
        """
    ).fetchall()
    return rows


def source_expr(name, path):
    if name == "duckdb":
        return "edges"
    if path.is_dir():
        return f"read_parquet('{path}/**/*.parquet', hive_partitioning=true)"
    return f"read_parquet('{path}')"


def time_query(variant, path, street, repeats=REPEATS):
    """Median wall time for the count + fetch pair the app issues."""
    times = []
    for _ in range(repeats):
        con = duckdb.connect(str(path)) if variant == "duckdb" else connect()
        if variant == "duckdb":
            for stmt in SETUP:
                con.execute(stmt)
        src = source_expr(variant, path)

        t0 = time.perf_counter()
        con.execute(
            f"SELECT count(*) FROM {src} WHERE FULLNAME = ?", [street]
        ).fetchone()
        con.execute(
            f"""SELECT {', '.join(COLS)}, geometry FROM {src}
                WHERE FULLNAME = ? LIMIT {MAX_DRAWN}""",
            [street],
        ).df()
        times.append(time.perf_counter() - t0)
        con.close()

    times.sort()
    return times[len(times) // 2]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true", help="build missing variants")
    ap.add_argument("--rebuild", action="store_true", help="rebuild all variants")
    ap.add_argument(
        "--only", nargs="*", default=None, help="restrict to these variants"
    )
    args = ap.parse_args()

    if not SOURCE.exists():
        raise SystemExit(f"source parquet not found at {SOURCE}")

    WORK.mkdir(exist_ok=True)
    (WORK / "spill").mkdir(exist_ok=True)

    names = list(VARIANTS) if args.only is None else args.only
    con = connect()
    spatial = has_spatial(con)

    # ---- build -----------------------------------------------------------
    built = {}
    for name in names:
        builder, pathfn = VARIANTS[name]
        path = pathfn()

        if name == "wkb" and not spatial:
            print(f"{name:14s} SKIP (spatial extension unavailable)")
            continue

        if builder is not None and (args.rebuild or not path.exists()):
            if not (args.build or args.rebuild):
                print(f"{name:14s} SKIP (not built; pass --build)")
                continue
            if path.exists():
                shutil.rmtree(path) if path.is_dir() else path.unlink()
            print(f"building {name} ...", flush=True)
            t0 = time.perf_counter()
            builder(con, path)
            print(f"  built in {time.perf_counter() - t0:.1f}s", flush=True)

        if path.exists():
            built[name] = path

    if not built:
        raise SystemExit("no variants available; pass --build")

    # ---- sizes -----------------------------------------------------------
    base = size_bytes(SOURCE)
    print(f"\n{'variant':14s} {'size':>10s} {'vs base':>9s}")
    for name, path in built.items():
        n = size_bytes(path)
        print(f"{name:14s} {n / 1e9:9.2f}G {n / base:8.2f}x")

    # ---- queries ---------------------------------------------------------
    targets = pick_names(con)
    print(f"\n{'variant':14s}" + "".join(f"{s[:14]:>16s}" for s, _ in targets))
    print(f"{'':14s}" + "".join(f"{f'({n:,} rows)':>16s}" for _, n in targets))

    for name, path in built.items():
        cells = []
        for street, _ in targets:
            cells.append(f"{time_query(name, path, street):15.3f}s")
        print(f"{name:14s}" + "".join(cells))

    print(
        "\nCaveat: the OS page cache is warm after the first pass, so these are "
        "warm-cache numbers. A deployed app reading cold from object storage "
        "will differ, and the gap between layouts will be larger there, not "
        "smaller."
    )


if __name__ == "__main__":
    main()
