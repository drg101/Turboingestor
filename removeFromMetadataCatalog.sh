> ./out/removeMongoCollection.js

echo "use sustaindb" >> ./out/removeMongoMetadatacatalogEntry.js
echo "db.Metadata.remove({collection: '$1'})" >> ./out/removeMongoMetadatacatalogEntry.js

mongo --port 27018 < ./out/removeMongoMetadatacatalogEntry.js