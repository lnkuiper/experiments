docker run -i --rm --link clickhouse_server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --multiquery < ../../schema/clickhouse_schema.sql

