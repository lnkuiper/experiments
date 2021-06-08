#!/bin/bash
. ../../pathvar.sh
cat $PATHVAR/data/randints/schema/schema.sql | docker exec -i monetdb-container mclient test
cat $PATHVAR/data/randints/load/monetdb_load.sql | docker exec -i monetdb-container mclient test
