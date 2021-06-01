#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Add --cpuset-cpus=0 to force single-threaded execution
docker run \
    --rm \
    --publish=5432:5432 \
    --volume /home/laurens/git/experiments/sorting/data:/sorting_data \
    --name umbra-container \
    --detach \
    umbra-image:latest

docker exec -i umbra-container /umbra/bin/sql --createdb test

