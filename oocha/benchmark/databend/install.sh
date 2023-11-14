#!/bin/bash
export SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
mkdir "$SCRIPT_DIR/databend" && cd "$SCRIPT_DIR/databend"

curl -LJO 'https://github.com/datafuselabs/databend/releases/download/v0.9.53-nightly/databend-v0.9.53-nightly-x86_64-unknown-linux-musl.tar.gz'
tar xzvf 'databend-v0.9.53-nightly-x86_64-unknown-linux-musl.tar.gz'
 
cat > config.toml << CONF
[storage]
type = "fs"

[storage.fs]
data_path = "./_data"

[meta]
endpoints = ["127.0.0.1:9191"]
username = "root"
password = "root"
auto_sync_interval = 60
CONF

# client_timeout_in_second = 60
