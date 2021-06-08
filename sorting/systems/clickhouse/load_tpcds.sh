docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --multiquery < $PATHVAR/data/randints/schema/clickhouse_schema.sql
../../data/tpcds/$SF/load/clickhouse_load.sh
