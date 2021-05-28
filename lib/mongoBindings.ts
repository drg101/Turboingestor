import mongodb = require('mongodb');
import assert = require('assert');
const { MongoClient }  = mongodb;
import { dbname, dbport } from './constants';
const url = `mongodb://localhost:${dbport}`;


MongoClient.connect(url, function(err, client) {
    assert.equal(null, err);
    console.log('Connected successfully to server');
    const db = client.db(dbname);
  
  });

