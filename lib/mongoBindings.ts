import mongodb = require('mongodb');
import assert = require('assert');
const { MongoClient } = mongodb;
import { dbname, dbport } from './constants';
import { execCommand } from './util';
const url = `mongodb://localhost:${dbport}`;



export const importCSV = async (collectionName: string, pathToCSV: string) => {
    await execCommand(`mongoimport --port ${dbport} --type csv -d ${dbname} -c ${collectionName} --file ${pathToCSV} --headerline`);
}

export const createIndexes = async (collectionName: string, indexes: string[]) => {
    const mongoIndexes = indexes.map(index => {
        return {
            key: {
                [ index ]: 1
            },
            name: index
        }
    });

    MongoClient.connect(url, async function (err, client) {
        if (err) {
            throw `Error connecting to mongodb @${url}`
        };
        const db = client.db(dbname);
        await db.collection(collectionName).createIndexes(mongoIndexes);
        client.close();
    });
}

