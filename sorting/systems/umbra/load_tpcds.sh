#!/bin/bash
docker exec -i umbra-container /umbra/bin/sql /umbra/my.db /sorting_data/tpcds/schema/schema.sql
docker exec -i umbra-container /umbra/bin/sql /umbra/my.db /sorting_data/tpcds/sf$SF/load/load.sql

docker exec \
    --detach \
    umbra-container \
    /umbra/bin/server \
    --address 0.0.0.0 \
    /umbra/my.db


