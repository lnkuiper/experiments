#!/bin/bash
. ../../pathvar.sh
docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --multiquery < $PATHVAR/data/randfloats/schema/clickhouse_schema.sql
echo "ClickHouse: CREATED RANDINTS SCHEMA"
cat $PATHVAR/data/randfloats/data/100asc.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats100_asc FORMAT CSV"
cat $PATHVAR/data/randfloats/data/100desc.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats100_desc FORMAT CSV"
cat $PATHVAR/data/randfloats/data/10.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats10 FORMAT CSV"
cat $PATHVAR/data/randfloats/data/20.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats20 FORMAT CSV"
cat $PATHVAR/data/randfloats/data/30.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats30 FORMAT CSV"
cat $PATHVAR/data/randfloats/data/40.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats40 FORMAT CSV"
cat $PATHVAR/data/randfloats/data/50.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats50 FORMAT CSV"
cat $PATHVAR/data/randfloats/data/60.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats60 FORMAT CSV"
cat $PATHVAR/data/randfloats/data/70.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats70 FORMAT CSV"
cat $PATHVAR/data/randfloats/data/80.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats80 FORMAT CSV"
cat $PATHVAR/data/randfloats/data/90.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats90 FORMAT CSV"
cat $PATHVAR/data/randfloats/data/100.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO floats100 FORMAT CSV"
echo "ClickHouse: COPIED RANDINTS DATA"

