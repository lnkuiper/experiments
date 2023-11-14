#!/bin/bash
export SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
mkdir "$SCRIPT_DIR/clickhouse" && cd "$SCRIPT_DIR/clickhouse"
curl https://clickhouse.com/ | sh
