#! /bin/bash

mongoport="27017"
mongohost="lattice-46"
mongodb="sustaindb"
collection="Metadata"
propertiesfile="config.properties"


if [ -d "./MetadataCatalog" ] 
then
    echo "Metadata Catalog already exists." 
else
    echo "Cloning Metadata Catalog"
    git clone "https://github.com/Project-Sustain/MetadataCatalog.git"
fi

cd MetadataCatalog
gradlew clean
gradlew build
sed -i "s/^mongodb.host=.*$/mongodb.host=$mongohost/" $propertiesfile
sed -i "s/^mongodb.port=.*$/mongodb.port=$mongoport/" $propertiesfile
sed -i "s/^mongodb.db=.*$/mongodb.db=$mongodb/" $propertiesfile
sed -i "s/^collection.names=.*$/collection.names=$1/" $propertiesfile
java -jar ./build/libs/MetadataCatalog-1.0-SNAPSHOT.jar
cd ..