#!/bin/bash
docker exec -it umbra-container /umbra/bin/sql /umbra/my.db /sorting_data/randints/schema/schema.sql
docker exec -it umbra-container /umbra/bin/sql /umbra/my.db /sorting_data/randints/load/load.sql

docker exec \
    --detach \
    umbra-container \
    /umbra/bin/server \
    --address 0.0.0.0 \
    /umbra/my.db

