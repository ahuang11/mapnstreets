import cartopy.crs as ccrs
import fugue.api as fa
import geopandas as gpd
import geoviews as gv
import panel as pn
from holoviews.streams import RangeXY
from shapely import wkt

gv.extension("bokeh")
pn.extension("tabulator")

INTRO = """
    *Have you ever looked at a street name and wondered how common it is?*

    Put your curiosity to rest with MapnStreets! By simply entering a name
    in the provided box, you can discover the prevalence of a street name.
    The map will display the locations of all streets with that name,
    and for more detailed information, you can click on the table to
    highlight their exact whereabouts.

    Uses [TIGER/Line® Edges](https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/EDGES/)
    data provided by the US Census Bureau.

    Powered by OSS:
    [Fugue](https://fugue-tutorials.readthedocs.io),
    [Panel](https://panel.holoviz.org/),
    [GeoPandas](https://geopandas.org/),
    [GeoViews](https://geoviews.org/),
    [Parquet](https://parquet.apache.org/),
    [DuckDB](https://duckdb.org/),
    [Ray](https://ray.io/),
    and all their supporting dependencies.
"""

QUERY_FMT = """
    df = LOAD "joined/*.parquet"
    df_sel = SELECT STATEFP, COUNTYFP, FULLNAME, geometry \
        FROM df WHERE FULLNAME == '{{name}}'
"""


class MapnStreets:
    def __init__(self):
        self.gdf = None
        self.name_input = pn.widgets.TextInput(
            value="*Andrew St",
            placeholder="Enter a name...",
            margin=(9, 5, 5, 25),
        )
        pn.bind(self.process_name, self.name_input, watch=True)

        features = gv.tile_sources.CartoDark()
        self.holoviews_pane = pn.pane.HoloViews(
            features, sizing_mode="stretch_both", min_height=800
        )
        self.tabulator = pn.widgets.Tabulator(width=225, disabled=True)
        self.records_text = pn.widgets.StaticText(value="<h3>0 records found</h3>")
        pn.state.onload(self.onload)

    def onload(self):
        self.name_input.param.trigger("value")

        range_xy = RangeXY()
        line_strings = gv.DynamicMap(
            self.refresh_line_strings, streams=[range_xy]
        ).opts(responsive=True)
        range_xy.source = line_strings

        points = gv.DynamicMap(
            pn.bind(self.refresh_points, self.tabulator.param.selection)
        ).opts(responsive=True)

        self.holoviews_pane.object *= line_strings * points

    def process_name(self, name):
        try:
            name = name.strip()
            self.holoviews_pane.loading = True
            query_fmt = QUERY_FMT
            if "*" in name or "%" in name:
                name = name.replace("*", "%")
                query_fmt = query_fmt.replace("==", "LIKE")
            if name == "%":
                return
            df = fa.as_pandas(
                fa.fugue_sql(query_fmt, name=name, engine="duckdb", as_local=True)
            )
            df["geometry"] = df["geometry"].apply(wkt.loads)
            self.gdf = gpd.GeoDataFrame(df)
            centroids = self.gdf["geometry"].centroid
            self.gdf["Longitude"] = centroids.x
            self.gdf["Latitude"] = centroids.y
            county_gdf = self.gdf.drop_duplicates(
                subset=["STATEFP", "COUNTYFP", "FULLNAME"]
            )
            records = len(county_gdf)
            self.records_text.value = f"<h3>{records} records found</h3>"
            self.tabulator.value = (
                county_gdf["FULLNAME"]
                .value_counts()
                .rename_axis("Name")
                .rename("Count")
                .to_frame()
            )
            self.refresh_line_strings()
        finally:
            self.holoviews_pane.loading = False

    def refresh_line_strings(self, x_range=None, y_range=None):
        line_strings = gv.Polygons(
            self.gdf[["geometry"]],
            crs=ccrs.PlateCarree(),
        ).opts(fill_alpha=0, line_color="white", line_width=8, alpha=0.6)
        return line_strings.select(x=x_range, y=y_range)

    def refresh_points(self, selection):
        gdf_selection = self.gdf[
            ["Longitude", "Latitude", "STATEFP", "COUNTYFP", "FULLNAME"]
        ]
        if self.tabulator.selection:
            names = self.tabulator.value.iloc[selection].index.tolist()
            gdf_selection = gdf_selection.loc[gdf_selection["FULLNAME"].isin(names)]
        points = gv.Points(
            gdf_selection,
            kdims=["Longitude", "Latitude"],
            vdims=["STATEFP", "COUNTYFP", "FULLNAME"],
            crs=ccrs.PlateCarree(),
        ).opts(marker="x", tools=["hover"], color="#FF4136", size=8)
        return points

    def view(self):
        template = pn.template.FastListTemplate(
            header=[pn.Row(self.name_input, self.records_text)],
            sidebar=[INTRO, self.tabulator],
            main=[
                self.holoviews_pane,
            ],
            theme="dark",
            title="MapnStreets",
            sidebar_width=225,
        )
        return template.servable()


mapn_streets = MapnStreets()
mapn_streets.view()
