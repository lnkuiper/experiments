#!/bin/bash
. ../../pathvar.sh
cat $PATHVAR/data/tpcds/schema/monetdb_schema.sql | sudo docker exec -i monetdb-container mclient test
cat $PATHVAR/data/tpcds/sf$SF/load/monetdb_load.sql | sudo docker exec -i monetdb-container mclient test
