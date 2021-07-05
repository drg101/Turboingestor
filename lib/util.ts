import csv = require('csv-parser');
import fs = require('fs');
import readline = require('readline');
import { exec } from 'child_process';
import fetch from 'node-fetch';

interface csvDictionary { [key: string]: string | number }
interface stringArrayDictionary { [key: string]: string[] }
interface stringDictionary { [key: string]: string }

export const getFieldsAndValidateCSV = (path: string) => {
    try {
        return new Promise<string[]>(resolve => {
            let fields = new Set<string>();
            let returnFields: string[] = [];
            fs.createReadStream(path)
                .pipe(csv())
                .on('data', (data) => { fields = new Set<string>([...fields, ...Object.keys(data)]) })
                .on('end', () => {
                    returnFields = [...fields]
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
    const newFileName = `${pathToCSV.substr(0, pathToCSV.length - 4)}_WITHOUT_DESCRIPTIVE_HEADERS_${Math.random().toString(36).substring(2, 6)}_.csv`;
    const labelMap = await popDescriptiveHeaderIntoLabelMap(pathToCSV, newFileName);
    console.log(labelMap)
    fs.writeFileSync(`./out/${name}LabelMap.json`, JSON.stringify(labelMap, null, 4))
    console.log(`Sucessfully created label map for ${pathToCSV}`)
    return newFileName;
}

export const exportLabelMapMulti = async (paths: string[], name: string = "defaultName") => {
    const filePaths = paths.map(async (fileName) => {
        return await exportLabelMap(fileName, name)
    })
    return await Promise.all(filePaths);
}

export const combineMultiyearCensusAndGetFilepath = async (pathsToFiles: string[], name: string = "defaultName") => {
    const newFilePath = `./out/${name}_combined_${randomString(4)}.csv`
    fs.openSync(newFilePath, 'w');
    let headerIsOn = false;
    for (const pathToFile of pathsToFiles) {
        const year = await getFirstEntryOfColumnOfCSV(pathToFile, "YEAR")
        const epoch_time = new Date(year).valueOf()
        const fileStream = fs.createReadStream(pathToFile);
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });
        let index = 0;
        for await (const line of rl) {
            if (index !== 0 || !headerIsOn) {
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
    return new Promise<string | number>(resolve => {
        const csvStream = fs.createReadStream(pathToCSV)
            .pipe(csv())
            .on('data', (data) => {
                csvStream.destroy();
                resolve(data[columnName])
            });
    })
}

export const normalizeCSVFiles = async (pathToCSVs: string[]) => {
    const labelMaps = await Promise.all(pathToCSVs.map(pathToCSV => getDescriptiveHeader(pathToCSV)))
    let deps: stringArrayDictionary = {}
    const finalLabelMap: csvDictionary = {}
    for (const labelMap of labelMaps) {
        for (const [code, label] of Object.entries(labelMap)) {
            if (!deps[label]) {
                deps[label] = [code]
                finalLabelMap[code] = label;
            }
            else {
                deps[label].push(code)
            }
        }
    }

    deps = Object.entries(deps).reduce((acc: stringArrayDictionary, [label, codes]) => {
        acc[codes[0]] = codes;
        return acc;
    }, {} as stringArrayDictionary)

    const invertedDeps: stringDictionary = Object.entries(deps).reduce((acc: stringDictionary, [masterCode, codes]) => {
        for(const code of codes){
            acc[code] = masterCode;
        }
        return acc;
    }, {} as stringDictionary)


    let newFiles: string[] = [];
    const randomRun = randomString(4)
    for (const pathToCSV of pathToCSVs) {
        const newFileName = `${pathToCSV.substr(0, pathToCSV.length - 4)}_NORMALIZED_${randomRun}.csv`
        newFiles.push(newFileName)
        fs.openSync(newFileName, 'w');
        const fileStream = fs.createReadStream(pathToCSV);
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });

        let index = 0;
        for await (const line of rl) {
            if (index === 0) {
                const labelMap = await getDescriptiveHeader(pathToCSV)
                for(const [key, master] of Object.entries(invertedDeps)){

                }
            }
            else if(index === 1){
                
            }
            else {
                fs.appendFileSync(newFileName, line + '\n');
            }
            index++;
        }
    }


    console.log(invertedDeps)
}

export const getMultiyearCensusFiles = (pathToFolder: string) => {
    return fs.readdirSync(pathToFolder).filter(fileName => fileName.match(/^.*\.csv$/g) && !fileName.match(/^.*WITHOUT_DESCRIPTIVE_HEADERS.*$/g)).map(filename => `${pathToFolder}/${filename}`)
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
        if (index !== 1) {
            //console.log(line + '\n')
            fs.appendFileSync(newFileName, line + '\n');
        }
        index++;
    }

    return await getDescriptiveHeader(pathToCSV)
}

const getDescriptiveHeader: (pathToCSV: string) => Promise<csvDictionary> = async (pathToCSV: string) => {
    return new Promise(resolve => {
        const readStream = fs.createReadStream(pathToCSV)
            .pipe(csv())
            .on('data', (data: csvDictionary) => { readStream.destroy(); resolve(data); })
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