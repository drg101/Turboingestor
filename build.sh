#! /bin/bash
set -e

npm run build

if [ -d "./MetadataCatalog" ] 
then
    echo "Metadata Catalog already exists." 
else
    echo "Cloning Metadata Catalog"
    git clone "https://github.com/Project-Sustain/MetadataCatalog.git"
    echo "Compiling Metadata Catalog"
    cd MetadataCatalog
    gradlew clean
    gradlew build
    cd ..
fi
