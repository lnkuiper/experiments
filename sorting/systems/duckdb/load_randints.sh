#!/bin/bash
. ../../pathvar.sh

if ! test -f "randints.db"; then
  cat $PATHVAR/data/randints/schema/schema.sql | $DUCKDB_BINARY randints.db
  sed 's@PATHVAR@'"$PATHVAR"'@' $PATHVAR/data/randints/load/duckdb_load.sql | $DUCKDB_BINARY randints.db
fi

