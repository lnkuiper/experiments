#!/bin/bash
. ../../pathvar.sh

# Add --cpuset-cpus=0 to force single-threaded execution
sudo docker run \
    --name postgres-container \
    --rm \
    --env "POSTGRES_PASSWORD=mysecretpassword" \
    --publish=5433:5432 \
    --volume $PATHVAR/data:/sorting_data \
    --detach \
    postgres

sleep 10
