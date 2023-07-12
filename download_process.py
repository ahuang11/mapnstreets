from typing import List
import warnings
from io import BytesIO
from pathlib import Path

import fugue.api as fa
import geopandas as gpd
import pandas as pd
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

JOINED_DIR = Path("joined")
JOINED_DIR.mkdir(exist_ok=True)


def fetch_links(url: str) -> List[str]:
    # Send a GET request to the URL
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception if the request fails

    # Create a BeautifulSoup object to parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")

    # List comprehension to get all the file URLs
    links = [
        url + link.get("href")
        for link in soup.find_all("a")
        if link.get("href") and link.get("href").endswith(".zip")
    ]
    return links


def download_and_process_shapefile(df: pd.DataFrame):
    for link in tqdm(df["link"], unit="file"):
        parquet_path = DATA_DIR / Path(link).with_suffix(".parquet").name
        done_path = parquet_path.with_suffix(".done")
        if done_path.exists():
            continue

        with requests.get(link) as response:
            response.raise_for_status()  # Raise an exception if the request fails
            shapefile_bytes = BytesIO(response.content)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            gdf = gpd.read_file(shapefile_bytes).assign(
                geometry=lambda gdf: gdf["geometry"].to_wkt()
            )

        gdf.to_parquet(parquet_path)
        del gdf
        done_path.touch()


if __name__ == "__main__":
    url = "https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/EDGES/"
    links = fetch_links(url)

    fa.out_transform(
        pd.DataFrame(dict(link=links)),
        download_and_process_shapefile,
        engine="ray",
        partition=8,
    )

    with fa.engine_context("ray"):
        df = fa.load(
            [str(path.absolute()) for path in DATA_DIR.glob("*.parquet")],
        )
        fa.save(fa.repartition(df, 50), str(JOINED_DIR.absolute()))
