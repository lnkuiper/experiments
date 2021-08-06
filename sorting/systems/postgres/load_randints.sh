#!/bin/bash
. ../../pathvar.sh

PGPASSWORD=mysecretpassword sudo docker exec -i postgres-container psql -U postgres -p 5433 -h localhost test -f /sorting_data/randints/schema/schema.sql
PGPASSWORD=mysecretpassword sudo docker exec -i postgres-container psql -U postgres -p 5433 -h localhost test -f /sorting_data/randints/load/load.sql
