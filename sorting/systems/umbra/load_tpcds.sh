#!/bin/bash
sudo docker exec -it umbra-container /umbra/bin/sql test /sorting_data/tpcds/schema/schema.sql
sudo docker exec -it umbra-container /umbra/bin/sql test /sorting_data/tpcds/sf$SF/load/load.sql

