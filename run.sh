#! /bin/bash
set -e
npm run build
echo Name = $1
echo Format = $2
echo Filepath = $3
echo Indexes = $4
echo
node build/ingest.js --name $1 --format $2 --filepath $3 --indexes $4

./updateMetadata.sh $1