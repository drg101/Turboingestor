import csv = require('csv-parser')
import fs = require('fs')

export const getFieldsAndValidateCSV = (path: string) => {
    try {
        return new Promise<string[]>(resolve => {
            let fields = new Set<string>();
            fs.createReadStream(path)
                .pipe(csv())
                .on('data', (data) => {fields = new Set<string>([...fields, ...Object.keys(data)])})
                .on('end', () => {
                    console.log(`Fields of ${path} are ${fields}`);
                });
            resolve([...fields]);
        })
    }
    catch (e) {
        console.error(e)
        console.error(`Bad CSV @${path}`);
    }
}