#!/bin/bash
docker exec -i umbra-container /umbra/bin/sql /dbs/my.db /sorting_data/randfloats/schema/schema.sql
docker exec -i umbra-container /umbra/bin/sql /dbs/my.db /sorting_data/randfloats/load/load.sql

docker exec \
    --detach \
    umbra-container \
    /umbra/bin/server \
    --address 0.0.0.0 \
    /dbs/my.db

sleep 10

