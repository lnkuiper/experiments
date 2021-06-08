cat $PATHVAR/data/randints/schema/schema.sql | $DUCKDB_BINARY randints.db
cat $PATHVAR/data/randints/load/load.sql | $DUCKDB_BINARY randints.db
