#!/bin/bash
. pathvar.sh

cd systems
sfs=(
  "10"
  "100"
  "300"
)

for sys in */ ; do
  if [ "$sys" == "duckdb/" ]; then
    continue
  fi
  if [ "$sys" == "hyper/" ]; then
    continue
  fi
  if [ "$sys" == "pandas/" ]; then
    continue
  fi
  if [ "$sys" == "sqlite3/" ]; then
    continue
  fi
  if [ "$sys" == "clickhouse/" ]; then
    continue
  fi
  echo "$sys"
  cd $sys
  FILE=${PATHVAR}/results/${sys}/randints.sql
  if ! test -f "$FILE"; then
    echo "$sys randints"
    #./stop.sh
    ./start.sh
    ./load_randints.sh && python3 randints_client.py && touch "$FILE"
    ./stop.sh
  fi
  for sf in "${sfs[@]}"; do
    FILE=${PATHVAR}/results/${sys}/tpcds_sf${sf}.sql
    if ! test -f "$FILE"; then
      echo "$sys tpcds sf $sf"
      export SF=${sf}
      #./stop.sh
      ./start.sh
      ./load_tpcds.sh && python3 tpcds_client.py && touch "$FILE"
      ./stop.sh
    fi
  done
  #./stop.sh
  cd ..
done
