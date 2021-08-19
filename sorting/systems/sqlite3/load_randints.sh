#!/bin/bash
. ../../pathvar.sh

cat $PATHVAR/data/randints/schema/schema.sql | sqlite3 randints.db
cat $PATHVAR/data/randints/load/sqlite3_load.sql | sqlite3 randints.db
