#!/bin/bash
. ../../pathvar.sh

psql -U raasveld -p 7484 -h localhost mydb -f $PATHVAR/data/randints/schema/schema.sql
sed 's@PATHVAR@'"$PATHVAR"'@' $PATHVAR/data/randints/load/duckdb_load.sql | psql -U raasveld -p 7484 -h localhost mydb

