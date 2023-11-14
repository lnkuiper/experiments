#!/bin/bash
sudo apt install python3 python3-pip cmake build-essential ninja-build postgresql databend_py -y
python3 -m pip install --user duckdb clickhouse_connect psycopg2 psycopg2-binary

