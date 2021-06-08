#!/bin/bash
. ../../pathvar.sh

cat $PATHVAR/data/tpcds/schema/schema.sql | $DUCKDB_BINARY tpcds_$SF.db
cat $PATHVAR/data/tpcds/$SF/load/load.sql | $DUCKDB_BINARY tpcds_$SF.db
