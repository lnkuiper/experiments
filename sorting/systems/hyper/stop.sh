#!/bin/bash
cat drop.sql | psql -U raasveld -p 7484 -h localhost mydb
sleep 1
killall hyperd > /dev/null 2>&1
sleep 3

