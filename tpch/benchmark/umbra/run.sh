#!/bin/bash
sudo mkdir -p /data/umbra
sudo rm -rf /data/umbra/db
sudo mkdir -p /data/umbra/db
sudo chmod 777 -R /data/umbra/db
docker run -d -v /data/umbra/db:/var/db -v /data:/data -p 5432:5432 --ulimit nofile=1048576:1048576 --ulimit memlock=8388608:8388608 umbradb/umbra:latest

sleep 5

start=$(date +%s%3N)
psql -p 5432 -h 127.0.0.1 -U postgres -f create-role.sql postgres
end=$(date +%s%3N)
echo "Load time: $(( (end - start) / 1000 ))"
