#!/bin/bash
docker stop umbra-container > /dev/null 2>&1
sleep 10
sudo rm -rf dbs/*

