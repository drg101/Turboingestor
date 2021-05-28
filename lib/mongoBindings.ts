import mongodb = require('mongodb');
import assert = require('assert');
const { MongoClient } = mongodb;
import { dbname, dbport } from './constants';
const url = `mongodb://localhost:${dbport}`;

let db: mongodb.Db;
let mclient: mongodb.MongoClient;
export const init = async () => {
    return new Promise<void>(resolve => {
        MongoClient.connect(url, function (err, client) {
            mclient = client;
            console.log('Connected successfully to the mongo router');
            db = client.db(dbname);
            console.log(`Connected to ${dbname}`)
            resolve();
        });
    });
}

export const done = () => {
    mclient.close();
}



