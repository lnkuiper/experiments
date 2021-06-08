#!/bin/bash
. ../../pathvar.sh

docker run \
    --rm \
    --publish=7484:7484 \
    --volume $PATHVAR/data:/sorting_data \
    --name hyper-container \
    --detach \
    --shm-size=8g \
    hyper-image:latest

echo "Waiting for the container to start..."
sleep 10
echo "Container started"

docker exec \
    --detach hyper-container \
    /hyper/hyperd --database /data/mydb --log-dir /hyper/ --config /hyper/config --skip-license --init-user raasveld --no-ssl --listen-connection tab.tcp://localhost:7484,tab.domain:///tmp/LD/domain/.s.PGSQL.7484 --no-password start

echo "Waiting for HyPer to start..."
sleep 10
echo "HyPer started"

echo "CREATE DATABASE test;"  | docker exec -i hyper-container psql -U raasveld -p 7484 -h localhost
