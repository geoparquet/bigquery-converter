import json
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List
import click
import geopandas as gpd
import numpy as np
import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
import pygeos
from numpy.typing import NDArray
from urllib.request import urlopen

AVAILABLE_COMPRESSIONS = ["NONE", "SNAPPY", "GZIP", "BROTLI", "LZ4", "ZSTD"]
GEOPARQUET_VERSION = "v1.0.0-beta.1"
GEOPARQUET_SCHEMA_URL = f"https://raw.githubusercontent.com/opengeospatial/geoparquet/{GEOPARQUET_VERSION}/format-specs/schema.json"

PygeosGeometryArray = NDArray[pygeos.Geometry]

class PathType(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""

    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))

class GeometryType(int, Enum):
    """Pygeos (GEOS) geometry type mapping
    From https://pygeos.readthedocs.io/en/latest/geometry.html?highlight=type#pygeos.geometry.get_type_id
    """

    Missing = -1
    Point = 0
    LineString = 1
    LinearRing = 2
    Polygon = 3
    MultiPoint = 4
    MultiLinestring = 5
    MultiPolygon = 6
    GeometryCollection = 7


def parse_to_pygeos(df: gpd.GeoDataFrame) -> Dict[str, PygeosGeometryArray]:
    """Parse to pygeos geometry array
    This is split out from _create_metadata so that we don't have to create the pygeos
    array twice: once for converting to wkb and another time for metadata handling.
    """
    geometry_columns: Dict[str, PygeosGeometryArray] = {}
    for col in df.columns[df.dtypes == "geometry"]:
        geometry_columns[col] = df[col].array.data

    return geometry_columns



def get_version() -> str:
    """Read the version const from the schema.json file"""
    schema = urlopen(GEOPARQUET_SCHEMA_URL)
    spec_schema = json.load(schema)
    return spec_schema["properties"]["version"]["const"]


def _create_metadata(
    df: gpd.GeoDataFrame, geometry_columns: Dict[str, PygeosGeometryArray]
) -> Dict[str, Any]:
    """Create and encode geo metadata dict.
    Parameters
    ----------
    df : GeoDataFrame
    Returns
    -------
    dict
    """

    # Construct metadata for each geometry
    column_metadata = {}
    for col, geometry_array in geometry_columns.items():
        geometry_types = get_geometry_types(geometry_array)
        bbox = list(pygeos.total_bounds(geometry_array))

        series = df[col]
        column_metadata[col] = {
            "encoding": "WKB",
            "geometry_types": geometry_types,
            "crs": series.crs.to_json_dict() if series.crs else None,
            "edges": "spherical",
            "bbox": bbox,
        }
    
    return {
        "version": get_version(),
        "primary_column": df._geometry_column_name,
        "columns": column_metadata,
    }


def get_geometry_types(pygeos_geoms: PygeosGeometryArray) -> List[str]:
    type_ids = pygeos.get_type_id(pygeos_geoms)
    unique_type_ids = set(type_ids)

    geom_type_names: List[str] = []
    for type_id in unique_type_ids:
        geom_type_names.append(GeometryType(type_id).name)

    return geom_type_names


def encode_metadata(metadata: Dict) -> bytes:
    """Encode metadata dict to UTF-8 JSON string
    Parameters
    ----------
    metadata : dict
    Returns
    -------
    UTF-8 encoded JSON string
    """
    # Remove unnecessary whitespace in JSON metadata
    # https://stackoverflow.com/a/33233406
    return json.dumps(metadata, separators=(',', ':')).encode("utf-8")


def geopandas_to_arrow(df: gpd.GeoDataFrame) -> pa.Table:
    geometry_columns = parse_to_pygeos(df)
    # BigQuery only support sphericals
    geo_metadata = _create_metadata(df, geometry_columns)

    df = pd.DataFrame(df)
    for col, geometry_array in geometry_columns.items():
        df[col] = pygeos.to_wkb(geometry_array)

    table = pa.Table.from_pandas(df, preserve_index=False)

    metadata = table.schema.metadata
    metadata.update({b"geo": encode_metadata(geo_metadata)})
    return table.replace_schema_metadata(metadata)



