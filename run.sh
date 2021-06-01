#! /bin/bash
set -e

if [ -z $4 ] 
then
    echo "Not enough parameters! Exiting."
    exit 1
fi

NOBUILD=false

if [[ $* == *-n* ]]
then
    echo "No build flag detected, not rebuilding."
        NOBUILD=true
fi

if [ "$NOBUILD" = false ]
then
    echo "Building stuff."
    ./build.sh
    echo
fi


echo Name = $1
echo Format = $2
echo Filepath = $3
echo Indexes = $4
echo
node build/ingest.js --name $1 --format $2 --filepath $3 --indexes $4

./updateMetadata.sh $1