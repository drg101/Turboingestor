npm run build

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
cd ..