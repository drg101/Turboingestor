

export default function ingestCensus(filepath: string, indexes: string[]) {
    if (!indexes.length) {
        indexes = [ 'GISJOIN' ];
    }
    console.log(`Ingesting ${filepath} as a census table. Indexing on [${indexes}]`);

    
}