#!/bin/bash
docker exec -it umbra-container /umbra/bin/sql test /sorting_data/tpcds/schema/schema.sql
docker exec -it umbra-container /umbra/bin/sql test /sorting_data/tpcds/sf$SF/load/load.sql

