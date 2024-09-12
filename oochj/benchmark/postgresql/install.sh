#!/bin/bash
sudo apt install build-essential libreadline-dev zlib1g-dev flex bison pkg-config -y
wget https://ftp.postgresql.org/pub/source/v16.4/postgresql-16.4.tar.gz
tar -xvzf postgresql-16.4.tar.gz
cd postgresql-16.4
./configure --prefix=/data/experiments/oochj/benchmark/postgresql/postgresql
make
make install

