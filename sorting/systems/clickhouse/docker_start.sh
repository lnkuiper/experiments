#!/bin/bash
. ../../pathvar.sh
sudo docker run --rm -d --name clickhouse_server -p 9001:9000 --ulimit nofile=262144:262144 yandex/clickhouse-server
sleep 10

