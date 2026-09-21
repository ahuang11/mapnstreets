# MapnStreets

An atlas of every street name in America, built on Metaflow and Panel.

## What it does

MapnStreets ingests all ~3,200 TIGER/Line EDGES shapefiles from the US Census
Bureau, sorts them into a single parquet file keyed by street name, and serves
a dashboard that lets you search any name and instantly see where it appears
nationwide.

## Architecture

```
┌─────────────────────┐      ┌──────────────────────────┐
│  MapnStreetsFlow     │      │  Panel dashboard         │
│  (flows/ingest/)    │      │  (deployments/dashboard/) │
│                     │      │                          │
│  download → process │─S3──▶│  DuckDB range reads      │
│  → sort → upload    │      │  datashader rasterize    │
└─────────────────────┘      └──────────────────────────┘
```

**Flow** — downloads TIGER shapefiles in parallel batches, converts to WKB
parquet parts, then sorts globally on `FULLNAME` so row-group statistics enable
predicate pushdown. Produces two artifacts: `edges.parquet` (~4 GB, geometry)
and `name_counts.parquet` (name → count lookup).

**Dashboard** — reads the latest successful run's parquet via S3 range requests
(or a local override). Searches are exact-match or prefix (`Oak*`) on the
sorted column. Results render as a datashader heatmap, an ECharts bar chart by
state, and a paginated record table.

## Running locally

```bash
# Run the ingest flow (on Kubernetes via fast-bakery)
python flows/ingest/flow.py --environment=fast-bakery run --max-num-splits 165

# Run the ingest flow (local, skip pypi_base)
OBPROJECT_SKIP_PYPI_BASE=1 python flows/ingest/flow.py run --url-limit 5

# Serve the dashboard
panel serve deployments/dashboard/app.py --dev --show

# Use a local parquet instead of S3
MAPNSTREETS_LOCAL=/tmp/mapnstreets_edges.parquet panel serve deployments/dashboard/app.py --dev --show
```

## Scripts

Utility scripts in `scripts/` (not deployed):

- `bench.py` — benchmark storage layouts (sorted vs partitioned vs indexed)
- `probe_query.py` — diagnose query performance against the run's parquet
- `probe_s3.py` — verify DuckDB can read S3 with brokered credentials
- `validate_urls.py` — sanity-check that Census Bureau URLs are live
- `test.py` — minimal test flow for platform smoke tests
