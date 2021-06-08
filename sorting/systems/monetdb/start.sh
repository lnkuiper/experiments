#!/bin/bash
. ../../pathvar.sh
docker run --rm --name monetdb-container --privileged -v /proc:/writable_proc --volume $PATHVAR/data:/sorting_data -e 'MONET_DATABASE=monetdb' -p 50000:50000 -d topaztechnology/monetdb:latest
docker exec -i --user root monetdb-container apk add sudo

docker cp .monetdb monetdb-container:/home/monetdb/.monetdb
docker cp clear_cache.sh monetdb-container:/clear_cache.sh

sleep 10
docker exec -i monetdb-container monetdb create test
docker exec -i monetdb-container monetdb start test

