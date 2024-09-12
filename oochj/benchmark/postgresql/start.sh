#!/bin/bash
export PGTEMP=`pwd`/temp
export TMPDIR=`pwd`/temp
pg_ctl -D data -l logfile start
