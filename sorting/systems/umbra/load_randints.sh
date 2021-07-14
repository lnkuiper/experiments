#!/bin/bash
sudo docker exec -it umbra-container /umbra/bin/sql test /sorting_data/randints/schema/schema.sql
sudo docker exec -it umbra-container /umbra/bin/sql test /sorting_data/randints/load/load.sql
