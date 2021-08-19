#!/bin/bash
. ../../pathvar.sh

cat $PATHVAR/data/tpcds/schema/schema.sql | sqlite3 tpcds_sf$SF.db -separator ','
cat $PATHVAR/data/tpcds/sf$SF/load/sqlite3_load.sql | sqlite3 tpcds_sf$SF.db -separator ','
