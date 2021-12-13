#!/bin/bash
. ../../pathvar.sh

psql -U raasveld -p 7484 -h localhost mydb -f $PATHVAR/data/tpcds/schema/schema.sql
sed 's@PATHVAR@'"$PATHVAR"'@' $PATHVAR/data/tpcds/sf$SF/load/hyper_load.sql | psql -U raasveld -p 7484 -h localhost mydb

