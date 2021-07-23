#!/bin/bash
. ../../pathvar.sh

cat $PATHVAR/data/tpcds/schema/schema.sql | $DUCKDB_BINARY tpcds_sf$SF.db
cat $PATHVAR/data/tpcds/sf$SF/load/duckdb_load.sql | $DUCKDB_BINARY tpcds_sf$SF.db

python3 load_tpcds.py

