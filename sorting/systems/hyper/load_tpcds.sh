#!/bin/bash
sudo docker exec -i hyper-container psql -U raasveld -p 7484 -h localhost test -f /sorting_data/tpcds/schema/schema.sql
sudo docker exec -i hyper-container psql -U raasveld -p 7484 -h localhost test -f /sorting_data/tpcds/sf$SF/load/load.sql
