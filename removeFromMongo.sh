> ./out/removeMongoCollection.js

echo "use sustaindb" >> ./out/removeMongoCollection.js
echo "db.$1.drop()" >> ./out/removeMongoCollection.js
echo "db.Metadata.remove({collection: '$1'})" >> ./out/removeMongoCollection.js

mongo --port 27018 < ./out/removeMongoCollection.js
