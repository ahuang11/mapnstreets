"""Diagnose the geometry fetch: why does pulling 95k rows take so long?

Runs the app's query against the run's parquet with the pieces separated, so
the cost lands on something specific instead of "the query is slow".

    python probe_query.py
    python probe_query.py "Oak St"
"""

import sys
import time

import duckdb
from metaflow import Flow, S3, namespace

STREET = sys.argv[1] if len(sys.argv) > 1 else "Main St"


def brokered_credentials():
    with S3() as s3:
        client = s3._s3_client.client
        creds = client._request_signer._credentials
        return creds.get_frozen_credentials(), client.meta.region_name


def timed(label, fn):
    t = time.perf_counter()
    out = fn()
    print(f"{label:38s} {time.perf_counter() - t:7.2f}s")
    return out


def main():
    namespace(None)
    run = Flow("MapnStreetsFlow").latest_successful_run
    edges = run.data.edges_url
    print(f"run   {run.id}")
    print(f"edges {edges}")

    with S3() as s3:
        info = s3.info(edges)
        print(f"size  {info.size / 1e9:.2f} GB\n")

    frozen, region = brokered_credentials()
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    fields = [
        "TYPE s3",
        f"KEY_ID '{frozen.access_key}'",
        f"SECRET '{frozen.secret_key}'",
        f"REGION '{region or 'us-west-2'}'",
    ]
    if frozen.token:
        fields.append(f"SESSION_TOKEN '{frozen.token}'")
    con.execute(f"CREATE SECRET probe ({', '.join(fields)});")

    # --- layout: are the row groups the size we asked for, and do the
    # FULLNAME statistics actually bracket ranges (i.e. is it really sorted)?
    print("--- layout ---")
    meta = con.execute(
        f"""
        SELECT row_group_id, row_group_num_rows,
               total_compressed_size, stats_min, stats_max
        FROM parquet_metadata('{edges}')
        WHERE path_in_schema = 'FULLNAME'
        ORDER BY row_group_id
        """
    ).df()
    print(f"row groups: {len(meta)}")
    if len(meta):
        print(f"rows/group: {meta.row_group_num_rows.iloc[0]:,}")
        print(
            "compressed/group (FULLNAME col): "
            f"{meta.total_compressed_size.mean() / 1e6:.1f} MB avg"
        )
        print("first 3 groups' FULLNAME ranges:")
        for _, r in meta.head(3).iterrows():
            print(f"  [{r.stats_min!r} .. {r.stats_max!r}]")

    geo = con.execute(
        f"""
        SELECT avg(total_compressed_size) AS avg_bytes
        FROM parquet_metadata('{edges}')
        WHERE path_in_schema = 'geometry'
        """
    ).fetchone()[0]
    print(f"geometry col per group: {geo / 1e6:.1f} MB avg\n")

    # --- the query, in pieces ---
    print(f"--- query for {STREET!r} ---")

    timed(
        "count(*) on edges",
        lambda: con.execute(
            f"SELECT count(*) FROM read_parquet('{edges}') WHERE FULLNAME = ?",
            [STREET],
        ).fetchone(),
    )

    # Attributes only: isolates predicate + row-group pruning from the cost of
    # moving the geometry blobs.
    timed(
        "attrs only, no geometry",
        lambda: con.execute(
            f"""SELECT STATEFP, COUNTYFP FROM read_parquet('{edges}')
                WHERE FULLNAME = ?""",
            [STREET],
        ).df(),
    )

    # Geometry bytes without materializing blobs as Python objects: separates
    # network transfer from the pandas conversion. octet_length, not length —
    # length() has no BLOB overload.
    timed(
        "geometry bytes only",
        lambda: con.execute(
            f"""SELECT sum(octet_length(geometry)) FROM read_parquet('{edges}')
                WHERE FULLNAME = ?""",
            [STREET],
        ).fetchone(),
    )

    for limit in (1_000, 25_000, 200_000):
        timed(
            f"full fetch .df() limit {limit:,}",
            lambda limit=limit: con.execute(
                f"""SELECT STATEFP, COUNTYFP, FULLNAME, geometry
                    FROM read_parquet('{edges}')
                    WHERE FULLNAME = ? LIMIT {limit}""",
                [STREET],
            ).df(),
        )

    # Arrow avoids the per-blob Python object conversion pandas does.
    timed(
        "full fetch .arrow() no limit",
        lambda: con.execute(
            f"""SELECT STATEFP, COUNTYFP, FULLNAME, geometry
                FROM read_parquet('{edges}')
                WHERE FULLNAME = ?""",
            [STREET],
        ).arrow(),
    )

    print("\n--- plan ---")
    plan = con.execute(
        f"""EXPLAIN ANALYZE
            SELECT STATEFP, COUNTYFP, FULLNAME, geometry
            FROM read_parquet('{edges}')
            WHERE FULLNAME = ? LIMIT 200000""",
        [STREET],
    ).fetchall()
    for row in plan:
        print(row[-1])


if __name__ == "__main__":
    main()
