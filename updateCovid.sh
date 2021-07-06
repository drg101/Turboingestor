#! /bin/bash
set -e

./download.sh COVID

./removeFromMongo.sh covid_county

./ingest.sh covid_county "--format census --filepath ./out/covid.csv --indexes GISJOIN epoch_time"

