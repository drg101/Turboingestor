#! /bin/bash
set -e

mongoport="27018"
mongohost="lattice-100"
mongodb="sustaindb"

cd out
> import.js
echo "use ${mongodb}" >> import.js
echo "db.$1.insert(" >> import.js
cat "../$2" >> import.js 
echo ")" >> import.js
mongo --port $mongoport < import.js
cd ..