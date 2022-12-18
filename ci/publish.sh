#!/bin/bash

mkdir -p public

python bigquery_to_parquet.py \
    --input-query "SELECT * FROM carto-do-public-data.carto.geography_usa_state_2019" \
    --mode FILE
    --output public/geography_usa_state_2019.parquet
