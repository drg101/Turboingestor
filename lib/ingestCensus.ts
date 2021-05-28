import { getFieldsAndValidateCSV } from './util';
import { importCSV, createIndexes } from './mongoBindings'  
import assert = require('assert');

export default async function ingestCensus(collectionName: string, filepath: string, indexes: string[]) {
    if (!indexes.length) {
        indexes = [ 'GISJOIN' ];
    }
    console.log(`Ingesting ${filepath} as a census table. Indexing on [${indexes}]`);

    const fieldsInCSV = await getFieldsAndValidateCSV(filepath);
    for (const index of indexes) {
        assert(fieldsInCSV.includes(index), "Specified index is not in dataset!!!")
    }
    
    await importCSV(collectionName, filepath);
    await createIndexes(collectionName, indexes)
}