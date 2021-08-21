#!/bin/bash
./clickhouse_client/clickhouse client --port 9001 --multiquery < drop.sql
killall clickhouse-server
sleep 10
