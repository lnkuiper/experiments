#!/bin/bash
. ../../pathvar.sh
FILE=tpcds_sf$SF.db
if ! test -f "$FILE"; then
  cat $PATHVAR/data/tpcds/schema/schema.sql | $DUCKDB_BINARY $FILE
  cat $PATHVAR/data/tpcds/sf$SF/load/duckdb_load.sql | $DUCKDB_BINARY $FILE
fi
