docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --multiquery < ../data/tpcds/clickhouse_schema.sql

