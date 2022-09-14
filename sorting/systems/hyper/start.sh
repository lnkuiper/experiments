#!/bin/bash
. ../../pathvar.sh

docker run \
    --rm \
    --publish=7484:7484 \
    --name hyper-container \
    --detach \
    --volume $PATHVAR/data:/sorting_data \
    hyper-image:latest

#cd hyper-binaries && ./run.sh > /dev/null 2>&1 & 
sleep 10

docker exec --detach hyper-container /hyper/hyperd --database /mydb --log-dir . --config /hyper/config --skip-license --init-user raasveld --listen-connection tab.tcp://localhost:7484,tab.domain:///tmp/LD/domain/.s.PGSQL.7484 --no-password start

sleep 10

