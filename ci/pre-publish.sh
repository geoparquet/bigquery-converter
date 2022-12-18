#!/bin/bash

mkdir -p public

TABLES=("carto-do-public-data.carto.geography_usa_state_2019" \
      "carto-do-public-data.carto.geography_usa_county_2019" \
      "carto-do-public-data.carto.geography_usa_censustract_2019" \
      "carto-do-public-data.carto.geography_usa_zcta5_2019" \ 
      "carto-do-public-data.carto.geography_usa_blockgroup_2019")

for table in ${TABLES[@]}; do
  python bigquery_to_parquet.py \
     --input-query "SELECT * FROM $table" \
     --mode FILE \
     --output "public/$table.parquet"
done
