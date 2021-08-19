#!/bin/bash
PGPASSWORD=mysecretpassword sudo docker exec -i postgres-container psql -U postgres -p 5433 -h localhost test -f /sorting_data/tpcds/schema/schema.sql
PGPASSWORD=mysecretpassword sudo docker exec -i postgres-container psql -U postgres -p 5433 -h localhost test -f /sorting_data/tpcds/sf$SF/load/load.sql
