#!/bin/bash
#./clickhouse_client/clickhouse client --port 9001 --multiquery < drop.sql > /dev/null 2>&1
clickhouse-client --port 9000 --multiquery < drop.sql > /dev/null 2>&1
#killall clickhouse-server > /dev/null 2>&1
sleep 10
