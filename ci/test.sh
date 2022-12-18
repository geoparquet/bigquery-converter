#/bin/bash

mkdir -p test

python bigquery_to_parquet.py \
    --input-query "SELECT * FROM carto-do-public-data.carto.geography_usa_state_2019" \
    --output test/geography_usa_state_2019

python parquet_to_bigquery.py \
    --input test/geography_usa_state_2019 \
    --output "cartodb-gcp-backend-data-team.geoparquet_ci.geography_usa_state_2019"
