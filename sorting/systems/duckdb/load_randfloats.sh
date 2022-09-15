#!/bin/bash
. ../../pathvar.sh

if ! test -f "randints.db"; then
  cat $PATHVAR/data/randfloats/schema/schema.sql | $DUCKDB_BINARY randfloats.db
  sed 's@PATHVAR@'"$PATHVAR"'@' $PATHVAR/data/randfloats/load/duckdb_load.sql | $DUCKDB_BINARY randfloats.db
fi

