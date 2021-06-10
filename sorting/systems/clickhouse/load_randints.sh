#!/bin/bash
. ../../pathvar.sh
docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --multiquery < $PATHVAR/data/randints/schema/clickhouse_schema.sql
echo "ClickHouse: CREATED RANDINTS SCHEMA"
cat $PATHVAR/data/randints/data/100asc.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints100_asc FORMAT CSV"
cat $PATHVAR/data/randints/data/100desc.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints100_desc FORMAT CSV"
cat $PATHVAR/data/randints/data/10.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints10 FORMAT CSV"
cat $PATHVAR/data/randints/data/20.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints20 FORMAT CSV"
cat $PATHVAR/data/randints/data/30.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints30 FORMAT CSV"
cat $PATHVAR/data/randints/data/40.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints40 FORMAT CSV"
cat $PATHVAR/data/randints/data/50.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints50 FORMAT CSV"
cat $PATHVAR/data/randints/data/60.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints60 FORMAT CSV"
cat $PATHVAR/data/randints/data/70.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints70 FORMAT CSV"
cat $PATHVAR/data/randints/data/80.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints80 FORMAT CSV"
cat $PATHVAR/data/randints/data/90.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints90 FORMAT CSV"
cat $PATHVAR/data/randints/data/100.csv | docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="INSERT INTO ints100 FORMAT CSV"
echo "ClickHouse: COPIED RANDINTS DATA"
