#!/bin/bash
#cd hyper
./hyperd --database mydb --log-dir . --config config --skip-license --init-user raasveld --listen-connection tab.tcp://localhost:7484,tab.domain:///tmp/LD/domain/.s.PGSQL.7484 --no-password run

