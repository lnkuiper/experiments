#!/bin/bash
. ../../pathvar.sh
clickhouse-client --port 9000 --multiquery < $PATHVAR/data/tpcds/schema/clickhouse_schema.sql
echo "ClickHouse: CREATED TPCDS SCHEMA"
cat $PATHVAR/data/tpcds/sf$SF/data/1_catalog_sales.csv | clickhouse-client --port 9000 --query="INSERT INTO catalog_sales FORMAT CSV"
cat $PATHVAR/data/tpcds/sf$SF/data/22_customer.csv | clickhouse-client --port 9000 --query="INSERT INTO customer FORMAT CSV"
echo "ClickHouse: COPIED TPCDS SF$SF DATA"
