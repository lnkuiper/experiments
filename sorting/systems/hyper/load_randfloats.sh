#!/bin/bash
. ../../pathvar.sh

docker exec -i hyper-container psql -U raasveld -p 7484 -h localhost mydb -f /sorting_data/randfloats/schema/schema.sql
sed 's@PATHVAR/data@'"/sorting_data/"'@' $PATHVAR/data/randfloats/load/duckdb_load.sql | docker exec -i hyper-container psql -U raasveld -p 7484 -h localhost mydb

