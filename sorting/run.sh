#!/bin/bash
cd systems
sfs=(
  "1"
  "10"
  "100"
  "300"
)

for sys in */ ; do
  cd $sys
  ./stop.sh
  ./start.sh
  ./load_randints.sh
  echo "$sys randints"
  python3 randints_client.py
  for sf in "${sfs[@]}"; do
    echo "$sys tpcds sf $sf"
    export SF=sf
    ./stop.sh
    ./start.sh
    ./load_tpcds.sh
    python3 tpcds_client.py
  done
  ./stop.sh
done
