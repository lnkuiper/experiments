#!/bin/bash
. ../../pathvar.sh

docker exec -it hyper-container psql -U raasveld -p 7484 -h localhost test -f /sorting_data/randints/schema/schema.sql
docker exec -it hyper-container psql -U raasveld -p 7484 -h localhost test -f /sorting_data/randints/load/load.sql