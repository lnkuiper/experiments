#!/bin/bash
. pathvar.sh

cd systems
sfs=(
  "1"
  "10"
  "100"
  "300"
)

for sys in */ ; do
  echo "$sys"
  if [ "$sys" == "umbra/" ]; then
    continue
  fi
  if [ "$sys" == "gnu/" ]; then
    continue
  fi
  cd $sys
  FILE=${PATHVAR}/results/${sys}/randints.sql
  if ! test -f "$FILE"; then
    ./stop.sh
    ./start.sh
    ./load_randints.sh
    echo "$sys randints"
    python3 randints_client.py && touch "$FILE"
  fi
  for sf in "${sfs[@]}"; do
    FILE=${PATHVAR}/results/${sys}/tpcds_sf${SF}.sql
    if ! test -f "$FILE"; then
      echo "$sys tpcds sf $sf"
      export SF=${sf}
      ./stop.sh
      ./start.sh
      ./load_tpcds.sh
      python3 tpcds_client.py && touch "$FILE"
      break
    fi
  done
  ./stop.sh
  cd ..
done
