docker run --rm -d --name clickhouse_server -p 8123:8123 --ulimit nofile=262144:262144 --volume=$PATHVAR/experiments/sorting/clickhouse/db:/var/lib/clickhouse yandex/clickhouse-server

