#! /bin/bash
set -e

if [ -z $4 ] 
then
    echo "Not enough parameters! Exiting."
    exit 1
fi

if [[ $* == *-n* ]]
then
    echo "No build flag detected, not rebuilding."
else
    echo "Building stuff."
    ./build.sh
    echo
fi

node --max_old_space_size=8192 build/ingest.js $2

./updateMetadata.sh $1