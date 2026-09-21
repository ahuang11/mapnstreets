"""MapnStreets: an atlas of every street name in America.

Reads sorted parquet from MapnStreetsFlow via S3 range requests.
Set MAPNSTREETS_LOCAL to a local edges.parquet to skip S3.

    python flow.py --environment=fast-bakery run --max-num-splits 165
    panel serve app.py --dev --show
"""

import asyncio
import os
import shutil
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path

import datashader as ds
import duckdb
import geopandas as gpd
import holoviews as hv
import panel as pn
import panel_material_ui as pmui
import param
import shapely
import spatialpandas as spd
from holoviews.operation import decimate
from holoviews.operation.datashader import dynspread, rasterize
from metaflow import Flow, S3, namespace
from panel.custom import Child, ReactComponent
from pyproj import Geod

hv.extension("bokeh")
pn.extension(
    "tabulator", "echarts", throttled=True, loading_indicator=True,
    notifications=True, loading_spinner="arc", loading_color="#b9761f",
)
pn.widgets.Tabulator.param.theme.default = "materialize"

_HERE = Path(__file__).parent
FLOW = "MapnStreetsFlow"
LEADERBOARD_SIZE = 15
BAR_TOP_STATES = 25
MAX_DRAWN = 200_000
DETAIL_THRESHOLD = 5_000
MAX_SAMPLES = 10_000
CONUS_X = (-14_000_000, -7_300_000)
CONUS_Y = (2_800_000, 6_400_000)
LOCAL_OVERRIDE = os.environ.get("MAPNSTREETS_LOCAL")
_GEOD = Geod(ellps="WGS84")
METERS_PER_MILE = 1609.344

STATE_NAMES = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut",
    "10": "Delaware", "11": "District of Columbia", "12": "Florida",
    "13": "Georgia", "15": "Hawaii", "16": "Idaho", "17": "Illinois",
    "18": "Indiana", "19": "Iowa", "20": "Kansas", "21": "Kentucky",
    "22": "Louisiana", "23": "Maine", "24": "Maryland",
    "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
    "28": "Mississippi", "29": "Missouri", "30": "Montana",
    "31": "Nebraska", "32": "Nevada", "33": "New Hampshire",
    "34": "New Jersey", "35": "New Mexico", "36": "New York",
    "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
    "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania",
    "44": "Rhode Island", "45": "South Carolina", "46": "South Dakota",
    "47": "Tennessee", "48": "Texas", "49": "Utah", "50": "Vermont",
    "51": "Virginia", "53": "Washington", "54": "West Virginia",
    "55": "Wisconsin", "56": "Wyoming", "60": "American Samoa",
    "66": "Guam", "69": "N. Mariana Islands", "72": "Puerto Rico",
    "78": "U.S. Virgin Islands",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def basemap():
    return hv.element.tiles.OSM().opts(
        show_grid=False, xaxis="bare", yaxis="bare", toolbar=None,
        bgcolor="#f6f3ea",
    )


def _filter_paths(paths, x_range, y_range):
    """Viewport slice for RangeXY stream."""
    if x_range is None or y_range is None:
        return paths
    try:
        return paths[slice(*x_range), slice(*y_range)]
    except Exception:
        return paths


def _detail_paths(paths, threshold=DETAIL_THRESHOLD):
    """Real line glyphs when zoomed in below *threshold* geometries."""
    return paths.iloc[:0] if len(paths) > threshold else paths


def _name_predicate(street):
    """SQL predicate for exact or prefix (*) match on sorted FULLNAME."""
    if not street.endswith("*"):
        return "FULLNAME = ?", [street]
    prefix = street[:-1]
    if not prefix:
        return None, None
    upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    return "FULLNAME >= ? AND FULLNAME < ?", [prefix, upper]


def _kpi_defaults(records="—", rank="—", length="—", states="—"):
    return {
        "records": records, "records_caption": "",
        "rank": rank, "rank_caption": "",
        "length": length, "length_caption": "",
        "states": states, "states_caption": "",
    }


def _state_bar_config(state_table):
    """ECharts horizontal bar chart from a State/Count DataFrame."""
    top = state_table.head(BAR_TOP_STATES)
    names = list(reversed(top["State"].tolist()))
    values = list(reversed(top["Count"].astype(int).tolist()))
    return {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "grid": {"left": 130, "right": 40, "top": 10, "bottom": 30},
        "xAxis": {"type": "value", "splitLine": {"lineStyle": {"color": "#ddd6c3"}}},
        "yAxis": {"type": "category", "data": names, "axisLabel": {"fontSize": 11}},
        "series": [{
            "type": "bar", "data": values,
            "itemStyle": {"color": "#b9761f", "borderRadius": [0, 3, 3, 0]},
            "barMaxWidth": 18,
            "label": {"show": True, "position": "right", "fontSize": 10, "color": "#6b6b60"},
        }],
    }


@dataclass
class SearchResult:
    total: int
    sel: "object | None"
    by_state: "object | None"
    rank: "int | None" = None
    matched_names: "int | None" = None
    distinct_names: "int | None" = None
    leaderboard: "object | None" = None


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------

def _brokered_credentials():
    """Credentials and region from Metaflow's S3 client."""
    with S3() as s3:
        client = s3._s3_client.client
        creds = client._request_signer._credentials
        if creds is None:
            raise RuntimeError("metaflow's S3 client has no credentials")
        return creds.get_frozen_credentials(), client.meta.region_name


def _fetch_local(url):
    """Download once to /tmp, reuse across sessions."""
    dest = Path(tempfile.gettempdir()) / "mapnstreets_edges.parquet"
    if not dest.exists():
        with S3() as s3:
            shutil.move(s3.get(url).path, dest)
    return str(dest)


def load_run():
    """Return (edges_url, counts_url, error_msg). Never raises."""
    try:
        namespace(None)
        run = Flow(FLOW).latest_successful_run
    except Exception as exc:
        return None, None, f"Could not reach Metaflow: `{exc}`"
    if run is None:
        return None, None, f"No successful run of `{FLOW}` found."
    try:
        return run.data.edges_url, run.data.counts_url, None
    except AttributeError:
        return None, None, f"Run `{run.id}` missing edges_url/counts_url."


# ---------------------------------------------------------------------------
# Shell (ReactComponent)
# ---------------------------------------------------------------------------

class Shell(ReactComponent):
    street = param.String(default="Main St")
    search_nonce = param.Integer(default=0)
    kpis = param.Dict(default=_kpi_defaults())
    count_message = param.String(default="")
    error = param.String(default="")
    teaser = param.String(default="")
    content = Child()

    _esm = _HERE / "shell.jsx"
    _stylesheets = [str(_HERE / "shell.css")]


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

class MapnStreets(pn.viewable.Viewer):
    def __init__(self, **params):
        self._map = pn.pane.HoloViews(
            basemap().redim.range(x=CONUS_X, y=CONUS_Y),
            sizing_mode="stretch_both", min_height=500,
        )
        self._records_table = pn.widgets.Tabulator(
            disabled=True, show_index=False, sizing_mode="stretch_both",
            page_size=15, pagination="local", layout="fit_data_stretch",
        )
        self._state_chart = pn.pane.ECharts(
            {}, sizing_mode="stretch_both", min_height=400,
            options={"replaceMerge": ["series"]},
        )

        super().__init__(**params)

        self._edges_url, self._counts_url, self._error = load_run()
        self._con = None
        self._edges = None
        self._counts = None
        self._init_lock = threading.Lock()
        self._distinct_names = None
        self._leaderboard = None

        self._tabs = pmui.Tabs(
            ("By state", self._state_chart),
            ("Map", self._map),
            ("Records", self._records_table),
            sizing_mode="stretch_both",
            dynamic=True,
        )
        self.shell = Shell(
            error=self._error or "",
            content=self._tabs,
            sizing_mode="stretch_both",
        )
        if self._edges_url is not None:
            pn.bind(self._update, self.shell.param.search_nonce, watch=True)
            pn.state.onload(self._eager_init)

    # -- Eager init ---------------------------------------------------------

    async def _eager_init(self):
        try:
            await asyncio.to_thread(self._warm_connection)
            if self._distinct_names:
                self.shell.teaser = (
                    f"{self._distinct_names:,} unique street names "
                    f"across {len(STATE_NAMES)} states and territories"
                )
        except Exception:
            pass  # will retry on first search

    def _warm_connection(self):
        with self._init_lock:
            if self._con is None:
                self._con = self._connect()
            if self._distinct_names is None:
                self._load_reference_stats()

    # -- Connection & reference data ----------------------------------------

    def _connect(self):
        con = duckdb.connect()
        if LOCAL_OVERRIDE:
            self._edges = f"'{LOCAL_OVERRIDE}'"
            self._counts = None
            return con
        try:
            frozen, region = _brokered_credentials()
            con.execute("INSTALL httpfs; LOAD httpfs;")
            fields = [
                "TYPE s3",
                f"KEY_ID '{frozen.access_key}'",
                f"SECRET '{frozen.secret_key}'",
                f"REGION '{region or 'us-west-2'}'",
            ]
            if frozen.token:
                fields.append(f"SESSION_TOKEN '{frozen.token}'")
            con.execute(f"CREATE SECRET mapnstreets ({', '.join(fields)});")
            con.execute(
                f"SELECT count(*) FROM read_parquet('{self._edges_url}')"
            ).fetchone()
            self._edges = f"'{self._edges_url}'"
            self._counts = f"'{self._counts_url}'"
        except Exception:
            self._edges = f"'{_fetch_local(self._edges_url)}'"
            self._counts = None
        return con

    def _load_reference_stats(self):
        if self._counts is not None:
            self._distinct_names = self._con.execute(
                f"SELECT count(*) FROM read_parquet({self._counts})"
            ).fetchone()[0]
            self._leaderboard = self._con.execute(
                f"SELECT FULLNAME AS \"Street name\", n AS Count "
                f"FROM read_parquet({self._counts}) "
                f"ORDER BY n DESC LIMIT {LEADERBOARD_SIZE}"
            ).df()
        else:
            self._distinct_names = self._con.execute(
                f"SELECT count(DISTINCT FULLNAME) FROM read_parquet({self._edges})"
            ).fetchone()[0]
            self._leaderboard = self._con.execute(
                f"SELECT FULLNAME AS \"Street name\", count(*) AS Count "
                f"FROM read_parquet({self._edges}) "
                f"GROUP BY FULLNAME ORDER BY Count DESC LIMIT {LEADERBOARD_SIZE}"
            ).df()

    # -- Query --------------------------------------------------------------

    def _rank_info(self, where, params, street, total):
        if street.endswith("*"):
            src = self._counts or self._edges
            if self._counts is not None:
                q = f"SELECT count(*) FROM read_parquet({self._counts}) WHERE {where}"
            else:
                q = (f"SELECT count(DISTINCT FULLNAME) "
                     f"FROM read_parquet({self._edges}) WHERE {where}")
            return None, self._con.execute(q, params).fetchone()[0]

        if self._counts is not None:
            rank = self._con.execute(
                f"SELECT count(*) + 1 FROM read_parquet({self._counts}) WHERE n > ?",
                [total],
            ).fetchone()[0]
        else:
            rank = self._con.execute(
                f"SELECT count(*) + 1 FROM ("
                f"  SELECT FULLNAME, count(*) AS n FROM read_parquet({self._edges})"
                f"  GROUP BY FULLNAME HAVING count(*) > ?)",
                [total],
            ).fetchone()[0]
        return rank, 1

    def _query(self, street):
        """Blocking: runs on a thread."""
        self._warm_connection()
        where, params = _name_predicate(street)
        if where is None:
            raise ValueError("A bare * would match every street; add a prefix")

        if self._counts is not None:
            row = self._con.execute(
                f"SELECT sum(n) FROM read_parquet({self._counts}) WHERE {where}",
                params,
            ).fetchone()
            total = int(row[0]) if row and row[0] is not None else 0
        else:
            total = self._con.execute(
                f"SELECT count(*) FROM read_parquet({self._edges}) WHERE {where}",
                params,
            ).fetchone()[0]

        if total == 0:
            return SearchResult(
                total=0, sel=None, by_state=None,
                distinct_names=self._distinct_names,
                leaderboard=self._leaderboard,
            )

        rank, matched_names = self._rank_info(where, params, street, total)

        by_state = self._con.execute(
            f"SELECT STATEFP, count(*) AS n "
            f"FROM read_parquet({self._edges}) WHERE {where} "
            f"GROUP BY STATEFP ORDER BY n DESC",
            params,
        ).df()

        sel = self._con.execute(
            f"SELECT STATEFP, COUNTYFP, FULLNAME, geometry "
            f"FROM read_parquet({self._edges}) WHERE {where} "
            f"LIMIT {MAX_DRAWN}",
            params,
        ).df()

        return SearchResult(
            total=total, sel=sel, by_state=by_state, rank=rank,
            matched_names=matched_names,
            distinct_names=self._distinct_names,
            leaderboard=self._leaderboard,
        )

    # -- Build results ------------------------------------------------------

    def _build_tables(self, sel, by_state):
        cols = [c for c in ("STATEFP", "COUNTYFP", "FULLNAME") if c in sel.columns]
        records = sel[cols].head(500).copy()
        if "STATEFP" in records.columns:
            records["State"] = records["STATEFP"].map(STATE_NAMES).fillna(records["STATEFP"])
        if "COUNTYFP" in records.columns:
            records = records.rename(columns={"COUNTYFP": "County FIPS"})
        records = records.rename(columns={"FULLNAME": "Street name"})
        records = records.drop(columns=["STATEFP"], errors="ignore")

        if "geometry" in sel.columns:
            head_geoms = shapely.from_wkb(sel["geometry"].head(500).map(bytes).to_numpy())
            records["Length (mi)"] = [
                round(_GEOD.geometry_length(g) / METERS_PER_MILE, 2) for g in head_geoms
            ]
            centroids = [g.centroid for g in head_geoms]
            records["Lat"] = [round(c.y, 4) for c in centroids]
            records["Lon"] = [round(c.x, 4) for c in centroids]

        if "Length (mi)" in records.columns:
            records = records.sort_values("Length (mi)", ascending=False).reset_index(drop=True)

        col_order = [c for c in ("Street name", "State", "County FIPS", "Length (mi)", "Lat", "Lon")
                     if c in records.columns]
        records = records[col_order]

        state_table = (
            by_state.assign(State=by_state["STATEFP"].map(STATE_NAMES).fillna(by_state["STATEFP"]))
            .rename(columns={"n": "Count"})[["State", "Count"]]
            .reset_index(drop=True)
        )
        return records, state_table

    def _build_map(self, sel):
        geoms_4326 = shapely.from_wkb(sel["geometry"].map(bytes).to_numpy())
        length_mi = sum(_GEOD.geometry_length(g) for g in geoms_4326) / METERS_PER_MILE

        gdf = gpd.GeoDataFrame(
            sel.drop(columns=["geometry"]), geometry=geoms_4326, crs="EPSG:4326",
        ).to_crs("EPSG:3857")

        paths = hv.Path(spd.GeoDataFrame(gdf[["geometry"]]))
        streams = [hv.streams.RangeXY(source=paths)]
        filtered = paths.apply(_filter_paths, streams=streams)
        sampled = decimate(filtered, max_samples=MAX_SAMPLES, streams=streams)
        shaded = rasterize(
            sampled, aggregator=ds.count(), line_width=1.5,
            precompute=True, streams=streams,
        )
        shaded = dynspread(shaded, max_px=3, threshold=0.4).opts(
            cmap=["#ffe873", "#ffd400", "#ff8c00"],
            cnorm="eq_hist", alpha=0.9, colorbar=True,
            clabel="segments per pixel",
        )
        detail = filtered.apply(
            _detail_paths, threshold=DETAIL_THRESHOLD
        ).opts(color="#ffd400", line_width=2, tools=["hover"])
        element = (basemap() * shaded * detail).redim.range(
            x=CONUS_X, y=CONUS_Y,
        ).opts(show_grid=False, xaxis="bare", yaxis="bare", toolbar=None)
        return element, length_mi

    # -- Search orchestration -----------------------------------------------

    async def _update(self, _nonce):
        street = self.shell.street
        if not street:
            return
        self.shell.count_message = f"Searching {street}..."
        with pn.io.hold():
            self._map.loading = True
            self._records_table.loading = True
            self._state_chart.loading = True
        try:
            await self._do_search(street)
        except Exception as exc:
            if street == self.shell.street:
                self.shell.count_message = f"Query failed: {exc}"
                self.shell.kpis = _kpi_defaults()
        finally:
            with pn.io.hold():
                self._map.loading = False
                self._records_table.loading = False
                self._state_chart.loading = False

    async def _do_search(self, street):
        result = await asyncio.to_thread(self._query, street)
        if street != self.shell.street:
            return

        if result.total == 0:
            self.shell.count_message = f"No streets named {street}"
            kpis = _kpi_defaults(records="0", states="0")
            kpis["rank_caption"] = "no matches"
            self.shell.kpis = kpis
            with pn.io.hold():
                self._records_table.value = None
                self._map.object = basemap().redim.range(x=CONUS_X, y=CONUS_Y)
                self._state_chart.object = {}
            return

        # Phase 1: tables + KPIs (instant).
        records, state_table = await asyncio.to_thread(
            self._build_tables, result.sel, result.by_state,
        )
        if street != self.shell.street:
            return

        capped = result.total > MAX_DRAWN
        if result.rank is not None:
            rank_value, rank_caption = f"#{result.rank:,}", f"of {result.distinct_names:,} names"
        else:
            rank_value, rank_caption = f"{result.matched_names:,}", "names matched (prefix)"

        kpis = {
            "records": f"{result.total:,}",
            "records_caption": f"of {MAX_DRAWN:,} shown on map" if capped else "matched",
            "rank": rank_value, "rank_caption": rank_caption,
            "length": "—", "length_caption": "computing...",
            "states": f"{len(state_table)}",
            "states_caption": f"of {len(STATE_NAMES)} tracked",
        }
        self.shell.count_message = (
            f"{result.total:,} records" + (f" — showing {MAX_DRAWN:,}" if capped else "")
        )
        self.shell.kpis = kpis
        with pn.io.hold():
            self._records_table.value = records
            self._state_chart.object = _state_bar_config(state_table)
        with pn.io.hold():
            self._records_table.loading = False
            self._state_chart.loading = False

        # Phase 2: full map (background).
        await asyncio.sleep(0)
        if street != self.shell.street:
            return
        element, length_mi = await asyncio.to_thread(self._build_map, result.sel)
        if street != self.shell.street:
            return

        self.shell.kpis = {
            **kpis,
            "length": f"{length_mi:,.0f} mi",
            "length_caption": f"of first {MAX_DRAWN:,} records" if capped else "total mapped",
        }
        await asyncio.sleep(0.001)
        self._map.object = element
        self._map.loading = False

    def __panel__(self):
        return self.shell


MapnStreets().servable(title="MapnStreets")
