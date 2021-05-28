import csv = require('csv-parser');
import fs = require('fs');
import { exec } from 'child_process';

export const getFieldsAndValidateCSV = (path: string) => {
    try {
        return new Promise<string[]>(resolve => {
            let fields = new Set<string>();
            let returnFields: string[] = [];
            fs.createReadStream(path)
                .pipe(csv())
                .on('data', (data) => { fields = new Set<string>([ ...fields, ...Object.keys(data) ]) })
                .on('end', () => {
                    returnFields = [ ...fields ]
                    console.log(`Fields of ${path} are ${returnFields}`);
                    resolve(returnFields);
                });
        })
    }
    catch (e) {
        console.error(e)
        console.error(`Bad CSV @${path}`);
        throw new Error("Bad CSV!");
    }
}

export const execCommand = async (cmd: string) => {
    return new Promise<boolean>(resolve => {
        exec(cmd, (err, stdout, stderr) => {
            if (err) {
                throw new Error(`Bad cli of ${cmd}`)
            }

            // the *entire* stdout and stderr (buffered)
            console.log(`stdout: ${stdout}`);
            console.log(`stderr: ${stderr}`);
            resolve(true);
        });
    })
}