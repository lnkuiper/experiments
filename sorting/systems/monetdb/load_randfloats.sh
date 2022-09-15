#!/bin/bash
. ../../pathvar.sh
cat $PATHVAR/data/randfloats/schema/schema.sql | docker exec -i monetdb-container mclient test
cat $PATHVAR/data/randfloats/load/monetdb_load.sql | docker exec -i monetdb-container mclient test

