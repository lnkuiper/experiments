#!/bin/bash

sfs=(
  "1"
  "10"
  "100"
  "300"
)

for sf in "${sfs[@]}"; do
  python3 dsdgen.py $sf
  break
done
