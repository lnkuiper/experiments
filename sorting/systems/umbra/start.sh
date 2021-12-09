#!/bin/bash
. ../../pathvar.sh

# Add --cpuset-cpus=0 to force single-threaded execution
docker run \
    --rm \
    --publish=5432:5432 \
    --volume $PATHVAR/data:/sorting_data \
    --name umbra-container \
    --detach \
    umbra-image:latest

docker exec \
    --interactive umbra-container /umbra/bin/sql \
    --createdb /umbra/my.db \
    /umbra/create-role.sql

sleep 10

