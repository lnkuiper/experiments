#!/bin/bash
. ../../pathvar.sh
cat $PATHVAR/data/randints/schema/schema.sql | sudo docker exec -i monetdb-container mclient test
cat $PATHVAR/data/randints/load/monetdb_load.sql | sudo docker exec -i monetdb-container mclient test
