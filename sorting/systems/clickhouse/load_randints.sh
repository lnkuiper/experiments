#!/bin/bash
. ../../pathvar.sh
./clickhouse_client/clickhouse client --port 9001 --multiquery < $PATHVAR/data/randints/schema/clickhouse_schema.sql
echo "ClickHouse: CREATED RANDINTS SCHEMA"
cat $PATHVAR/data/randints/data/100asc.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints100_asc FORMAT CSV"
cat $PATHVAR/data/randints/data/100desc.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints100_desc FORMAT CSV"
cat $PATHVAR/data/randints/data/10.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints10 FORMAT CSV"
cat $PATHVAR/data/randints/data/20.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints20 FORMAT CSV"
cat $PATHVAR/data/randints/data/30.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints30 FORMAT CSV"
cat $PATHVAR/data/randints/data/40.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints40 FORMAT CSV"
cat $PATHVAR/data/randints/data/50.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints50 FORMAT CSV"
cat $PATHVAR/data/randints/data/60.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints60 FORMAT CSV"
cat $PATHVAR/data/randints/data/70.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints70 FORMAT CSV"
cat $PATHVAR/data/randints/data/80.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints80 FORMAT CSV"
cat $PATHVAR/data/randints/data/90.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints90 FORMAT CSV"
cat $PATHVAR/data/randints/data/100.csv | ./clickhouse_client/clickhouse client --port 9001 --query="INSERT INTO ints100 FORMAT CSV"
echo "ClickHouse: COPIED RANDINTS DATA"
