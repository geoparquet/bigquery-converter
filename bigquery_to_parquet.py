import click
import sys
import pyarrow.parquet as pq
import shutil
from os.path import exists

from encoder import AVAILABLE_COMPRESSIONS, PathType, geopandas_to_arrow
from pathlib import Path
from google.cloud import bigquery

MODES = ["FILE", "FOLDER"]

def read_gdf(query: str):
    client = bigquery.Client()
    df = client.query(query).to_geodataframe()
    return df

@click.command()
@click.option(
    "-q",
    "--input-query",
    type=str,
    help="SQL query of the data to export",
    required=True,
)
@click.option(
    "-o",
    "--output",
    type=PathType(file_okay=True, dir_okay=True, writable=True),
    help="Path to output",
    required=True,
)
@click.option(
    "-m",
    "--mode",
    type=click.Choice(MODES, case_sensitive=False),
    help="Mode to use FILE or FOLDER",
    default=MODES[1],
    show_default=True
)
@click.option(
    "--compression",
    type=click.Choice(AVAILABLE_COMPRESSIONS, case_sensitive=False),
    default="SNAPPY",
    help="Compression codec to use when writing to Parquet.",
    show_default=True,
)
@click.option(
    "--file-max-records",
    type=int,
    default=5000,
    help="Max number of records per file in FOLDER mode. It's ignored in FILE mode.",
    show_default=True,
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite output folder.",
    show_default=True,
)
def main(input_query: str, output: Path, mode: str, compression: str , file_max_records: int, overwrite: bool):
    
    if exists(output):
        if overwrite==False:
            print(f"{output} exists and overwrite is set to false")
            sys.exit(-1)
        else:
            shutil.rmtree(output)

    print("Reading data from BigQuery")
    if mode.upper() == 'FOLDER':
        gdf = (
            read_gdf(input_query)
                .assign(__partition__= lambda x: x.index // file_max_records)
        )
    else:
        gdf = read_gdf(input_query)

    print("Converting to GeoParquet")
    arrow_table = geopandas_to_arrow(gdf)

    if mode.upper() == 'FOLDER':
        # We need to export to multiple files, because a single file might hit bigquery limits (UDF out of memory). https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet
        pq.write_to_dataset(arrow_table, root_path=output, partition_cols=['__partition__'], compression=compression)
    else:
        pq.write_table(arrow_table, output, compression=compression)

    print(f"Successfully created at {output}")


if __name__ == "__main__":
    main()
