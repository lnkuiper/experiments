#!/bin/bash
. ../../pathvar.sh
docker run --rm -d --name clickhouse_server -p 9000:9000 --ulimit nofile=262144:262144 yandex/clickhouse-server
sleep 10

