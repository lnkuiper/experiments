#!/bin/bash
. ../../pathvar.sh
docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --multiquery < $PATHVAR/data/randints/schema/clickhouse_schema.sql
echo "ClickHouse: CREATED TPCDS SCHEMA"
cat "$PATHVAR/data/tpcds/sf$SF/data/1_catalog_sales.csv" | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO catalog_sales FORMAT CSV"
cat "$PATHVAR/data/tpcds/sf$SF/data/22_customer.csv" | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO customer FORMAT CSV"
echo "ClickHouse: COPIED TPCDS SF$SF DATA"