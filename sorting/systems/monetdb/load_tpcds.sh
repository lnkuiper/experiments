#!/bin/bash
. ../../pathvar.sh
cat $PATHVAR/data/tpcds/monetdb_schema.sql | docker exec -i monetdb-container mclient test
cat $PATHVAR/data/tpcds/sf$SF/load/monetdb_load.sql | docker exec -i monetdb-container mclient test
