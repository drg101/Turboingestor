import csv = require('csv-parser');
import fs = require('fs');
import readline = require('readline');
import { exec } from 'child_process';
import fetch from 'node-fetch';

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

export const exportLabelMapMulti = async (pathToFolder: string, name: string = "defaultName") => {
    console.log(`Creating label map for ${pathToFolder}`)
    const filePaths = getMutiyearCensusFiles(pathToFolder).map(async (fileName) => {
            return await exportLabelMap(`${pathToFolder}/${fileName}`, name)
        })
    return await Promise.all(filePaths);
}

export const combineMultiyearCensusAndGetFilepath = async (pathsToFiles: string[], name: string = "defaultName") => {
    const newFilePath = `./out/${name}_combined_${randomString(4)}.csv`
    fs.openSync(newFilePath, 'w');
    let headerIsOn = false;
    for(const pathToFile of pathsToFiles){
        const year = await getFirstEntryOfColumnOfCSV(pathToFile, "YEAR")
        const epoch_time = new Date(year).valueOf()
        const fileStream = fs.createReadStream(pathToFile);
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });
        let index = 0;
        for await (const line of rl) {
            if(index !== 0 || !headerIsOn){
                headerIsOn = true;
                fs.appendFileSync(newFilePath, `${index === 0 ? "epoch_time" : epoch_time},${line}\n`);
            }
            index++;
        }
        await execCommand(`rm -f ${pathToFile}`);
    }
    return newFilePath;
}

const getFirstEntryOfColumnOfCSV = async (pathToCSV: string, columnName: string) => {
    return new Promise<string|number>(resolve => {
        const csvStream = fs.createReadStream(pathToCSV)
                .pipe(csv())
                .on('data', (data) => { 
                    csvStream.destroy(); 
                    resolve(data[columnName])
                });
    })
}

const getMutiyearCensusFiles = (pathToFolder: string) => {
    return fs.readdirSync(pathToFolder).filter(fileName => fileName.match(/^.*\.csv$/g) && !fileName.match(/^.*WITHOUT_DESCRIPTIVE_HEADERS.*$/g))
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

export const getJSON = async (URL: string) => {
    let response = await fetch(URL);
    let data = await response.json()
    return data;
}