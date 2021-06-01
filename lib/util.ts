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
    const labelMap = await popDescriptiveHeaderIntoLabelMap(pathToCSV);
    console.log(labelMap)
    fs.writeFile(`./out/${name}LabelMap.json`, JSON.stringify(labelMap, null, 4), err => {
        if (err) {
            console.error(err);
            throw "Bad Label map write!";
        }
        console.log(`Sucessfully created label map for ${pathToCSV}`)
    })
}

export const popDescriptiveHeaderIntoLabelMap = async (pathToCSV: string) => {
    const newFileName = `${pathToCSV.substr(0,pathToCSV.length-3)}_WITH_DESCRIPTIVE_HEADERS.csv`
    fs.copyFileSync(pathToCSV, newFileName);
    let CSVContent = fs.readFileSync(newFileName).toString().split('\n'); // read file and convert to array by line break
    CSVContent.splice(1,1);
    fs.writeFileSync(pathToCSV, CSVContent.join('\n'));

    return new Promise(resolve => {
        const readStream = fs.createReadStream(newFileName)
                .pipe(csv())
                .on('data', (data) => { readStream.destroy(); resolve(data);  })
    })
}