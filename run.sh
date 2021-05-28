#! /bin/bash
npm run build
echo Format = $1
echo Filepath = $2
echo Indexes = $3
echo
node build/ingest.js --format $1 --filepath $2 --indexes $3