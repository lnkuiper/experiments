#!/bin/bash
. ../../pathvar.sh
#docker run --rm -d --name clickhouse_server -p 9000:9000 --ulimit nofile=262144:262144 --volume=$PATHVAR/systems/clickhouse/db:/var/lib/clickhouse yandex/clickhouse-server
docker run --rm -d --name clickhouse_server -p 9000:9000 --ulimit nofile=262144:262144 yandex/clickhouse-server