#!/bin/bash
docker exec -it umbra-container /umbra/bin/sql test /sorting_data/randints/schema/schema.sql
docker exec -it umbra-container /umbra/bin/sql test /sorting_data/randints/load/load.sql

