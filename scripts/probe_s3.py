"""Probe: can DuckDB read the run's parquet directly from S3?

The brokered credentials Metaflow uses are not the ones a fresh boto3.Session()
resolves, so this pulls them off Metaflow's own configured client and hands them
to DuckDB. Prints what it finds at each step so a failure says where.

    python probe_s3.py
"""

import duckdb
from metaflow import Flow, S3, namespace


def metaflow_credentials():
    """Credentials and region from Metaflow's own S3 client.

    Private API: metaflow.plugins.datatools.s3.s3.S3._s3_client.client is a
    boto3 client, and botocore hangs the resolved credentials off its request
    signer. Verified against the paths in the traceback rather than docs, so
    check this still holds after a metaflow upgrade.
    """
    with S3() as s3:
        client = s3._s3_client.client
        region = client.meta.region_name
        endpoint = client.meta.endpoint_url
        creds = client._request_signer._credentials
        if creds is None:
            raise RuntimeError("metaflow's client has no credentials attached")
        frozen = creds.get_frozen_credentials()
    return frozen, region, endpoint


def main():
    namespace(None)
    run = Flow("MapnStreetsFlow").latest_successful_run
    url = run.data.edges_url
    print(f"run       {run.id}")
    print(f"url       {url}")

    frozen, region, endpoint = metaflow_credentials()
    print(f"region    {region}")
    print(f"endpoint  {endpoint}")
    print(f"key_id    {frozen.access_key[:4]}...{frozen.access_key[-4:]}")
    print(f"token     {'yes' if frozen.token else 'no'}")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    fields = [
        "TYPE s3",
        f"KEY_ID '{frozen.access_key}'",
        f"SECRET '{frozen.secret_key}'",
        f"REGION '{region}'",
    ]
    if frozen.token:
        fields.append(f"SESSION_TOKEN '{frozen.token}'")
    con.execute(f"CREATE SECRET probe ({', '.join(fields)});")

    # Footer-only: count(*) on parquet reads metadata, not row groups. If the
    # credentials work at all, this is a couple of ranged requests.
    print("\nreading footer ...", flush=True)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{url}')").fetchone()[0]
    print(f"rows      {n:,}")

    # A real predicate: this is what the app issues.
    print("\nquerying 'Main St' ...", flush=True)
    import time

    t0 = time.perf_counter()
    hits = con.execute(
        "SELECT count(*) FROM read_parquet(?) WHERE FULLNAME = ?",
        [url, "Main St"],
    ).fetchone()[0]
    print(f"matches   {hits:,} in {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    main()
