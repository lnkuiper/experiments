#!/bin/bash
. ../../pathvar.sh
sudo docker run --rm --name monetdb-container --privileged -v /proc:/writable_proc --volume $PATHVAR/data:/sorting_data -e 'MONET_DATABASE=monetdb' -p 50000:50000 -d topaztechnology/monetdb:latest
sudo docker exec -i --user root monetdb-container apk add sudo

sudo docker cp .monetdb monetdb-container:/home/monetdb/.monetdb
sudo docker cp clear_cache.sh monetdb-container:/clear_cache.sh

sleep 10
sudo docker exec -i monetdb-container monetdb create test
sudo docker exec -i monetdb-container monetdb start test
sleep 10
