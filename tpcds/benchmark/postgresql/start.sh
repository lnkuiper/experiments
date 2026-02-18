#!/bin/bash
export PGTEMP=`pwd`/temp
export TMPDIR=`pwd`/temp
./postgresql/bin/pg_ctl -D data -l logfile start
