import csv = require('csv-parser');
import fs = require('fs');
import readline = require('readline');
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

export const exportLabelMap = async (pathToCSV: string, name: string = "defaultName") => {
    console.log(`Creating label map for ${pathToCSV}`)
    const newFileName = `${pathToCSV.substr(0, pathToCSV.length - 4)}_WITHOUT_DESCRIPTIVE_HEADERS_${Math.random().toString(36).substring(2,6)}_.csv`;
    const labelMap = await popDescriptiveHeaderIntoLabelMap(pathToCSV, newFileName);
    console.log(labelMap)
    fs.writeFileSync(`./out/${name}LabelMap.json`, JSON.stringify(labelMap, null, 4))
    console.log(`Sucessfully created label map for ${pathToCSV}`)
    return newFileName;
}

export const popDescriptiveHeaderIntoLabelMap = async (pathToCSV: string, newFileName: string) => {
    fs.openSync(newFileName, 'w');
    const fileStream = fs.createReadStream(pathToCSV);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });
    let index = 0;
    for await (const line of rl) {
        if(index !== 1){
            //console.log(line + '\n')
            fs.appendFileSync(newFileName, line + '\n');
        }
        index++;
    }

    return new Promise(resolve => {
        const readStream = fs.createReadStream(pathToCSV)
            .pipe(csv())
            .on('data', (data) => { readStream.destroy(); resolve(data); })
    })
}

export const randomString = (length: number) => {
    return Math.random().toString(36).substring(2, 2 + length);
}