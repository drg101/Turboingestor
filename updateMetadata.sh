#! /bin/bash
set -e

mongoport="27018"
mongohost="lattice-100"
mongodb="sustaindb"
collection="Metadata"
propertiesfile="config.properties"

cd MetadataCatalog
sed -i "s/^mongodb.host=.*$/mongodb.host=$mongohost/" $propertiesfile
sed -i "s/^mongodb.port=.*$/mongodb.port=$mongoport/" $propertiesfile
sed -i "s/^mongodb.db=.*$/mongodb.db=$mongodb/" $propertiesfile
sed -i "s/^collection.names=.*$/collection.names=$1/" $propertiesfile
java -jar ./build/libs/MetadataCatalog-1.0-SNAPSHOT.jar

# this is pretty cute if you ask me.
> import.js
echo "use ${mongodb}" >> import.js
echo "db.${collection}.insert(" >> import.js
cat metadata.json >> import.js 
echo ")" >> import.js
mongo --port $mongoport < import.js
cd ..
