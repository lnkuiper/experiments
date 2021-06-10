#!/bin/bash
. ../../pathvar.sh

cat $PATHVAR/data/tpcds/schema/schema.sql | $DUCKDB_BINARY tpcds_sf$SF.db
cat $PATHVAR/data/tpcds/sf$SF/load/load.sql | $DUCKDB_BINARY tpcds_sf$SF.db
