#!/bin/bash
. ../../pathvar.sh

docker exec -i hyper-container psql -U raasveld -p 7484 -h localhost mydb -f /sorting_data/tpcds/schema/schema.sql
cat $PATHVAR/data/tpcds/sf$SF/load/hyper_load.sql | docker exec -i hyper-container psql -U raasveld -p 7484 -h localhost mydb

