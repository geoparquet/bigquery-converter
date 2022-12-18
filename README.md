# bigquery-converter

Experimental python scripts to import/export [GeoParquet](https://geoparquet.org) files in [Google BigQuery](https://cloud.google.com/bigquery).

## Motivation
BigQuery supports parquet files, but for tables with geometry data it's not supported. For example, if you try to export a table with a geography column it throws the following error:
>Error while reading data, error message: Type GEOGRAPHY is not currently supported for parquet exports.

We've created these experimental scripts to improve this, but the ultimate goal is that Google BigQuery adopts GeoParquet as the standard to encode geospatial data inside parquet. Thus, once implemented, the current tools of BigQuery ([load](https://cloud.google.com/bigquery/docs/loading-data) and [export](https://cloud.google.com/bigquery/docs/exporting-data)) will replace these scripts.

## Installation

To use these scripts you need Python>3.8. You can download Python [here](https://www.python.org/downloads/).

Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Convert a SQL query to parquet:

```bash
python bigquery_to_parquet.py \
    --input-query "SELECT * FROM carto-do-public-data.carto.geography_usa_state_2019" \
    --output geography_usa_state_2019
```

Upload a parquet file to BigQuery:

```bash
python parquet_to_bigquery.py \
    --input geography_usa_state_2019 \
    --output "cartodb-gcp-backend-data-team.alasarr.geography_usa_state_2019"
```

These are the paremeters for bigquery_to_parquet:

```
Options:
  -q, --input-query TEXT          SQL query of the data to export  [required]
  -o, --output PATH               Path to output  [required]
  -m, --mode [FILE|FOLDER]        Mode to use FILE or FOLDER  [default:
                                  FOLDER]
  --compression [NONE|SNAPPY|GZIP|BROTLI|LZ4|ZSTD]
                                  Compression codec to use when writing to
                                  Parquet.  [default: SNAPPY]
  --file-max-records INTEGER      Max number of records per file in FOLDER
                                  mode. It's ignored in FILE mode.  [default:
                                  5000]
  --overwrite                     Overwrite output folder.
  --help                          Show this message and exit.   
```

These are the paremeters for parquet_to_bigquery:

```
Options:
  -i, --input PATH   Path to a parquet file or a folder with multiple parquet
                     files inside (it requires extension *.parquet).
                     [required]
  -o, --output TEXT  FQN of the destination table (project.dataset.table).
                     [required]
  --help             Show this message and exit.
```

### Modes

There are two modes in these scripts: 

- `FOLDER`(default): it creates many GeoParquet out of your query. The size of these files are controlled by `file-max-records` parameters.
- `FILE`: it works with a single file. If you're working with large tables in BigQuery and you use this mode, you might hit [bigquery limits](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet) when you upload it to BigQuery. 


Convert a SQL query to single parquet file:

```bash
python bigquery_to_parquet.py \
    --input-query "SELECT * FROM carto-do-public-data.carto.geography_usa_blockgroup_2019" \
    --primary-column geom \
    --mode FILE \
    --output geography_usa_blockgroup_2019.parquet
```

### Known issues and limitations

- `bigquery_to_parquet` only supports queries with 1 geography column. 
- `bigquery_to_parquet` is limited to the memory of the user. It executes a query and converts the results to [Geopandas](https://geopandas.org/), so the query results have to fit in memory. 