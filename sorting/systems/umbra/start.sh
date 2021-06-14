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

docker cp create-role.sql umbra-container:/create-role.sql

docker exec \
    --interactive umbra-container /umbra/bin/sql \
    --createdb test \
    create-role.sql

docker exec \
    --detach umbra-container \
    /umbra/bin/server -address 0.0.0.0 test

sleep 10
