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

interface labelMap {
    [elementName: string]: string;
}

export const popDescriptiveHeaderIntoLabelMap = async (pathToCSV: string) => {
    const fileStream = fs.createReadStream(pathToCSV);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let lineNum = 0;
    let lineArrs: string[][] = [];
    for await (const line of rl) {
        if(lineNum < 2){
            lineArrs.push(line.split(','));
        }
        else{
            break;
        }
        lineNum++;
    }
    console.log(`Got label map values for ${pathToCSV}`)

    //TODO: make this use a stream cause this is gross
    console.log(`Copying old file before pop.`)
    fs.copyFileSync(pathToCSV,`${pathToCSV}_WITH_DESCRIPTIVE_HEADERS`);
    let CSVContent = fs.readFileSync(pathToCSV).toString().split('\n'); // read file and convert to array by line break
    CSVContent.splice(1,1);
    fs.writeFileSync(pathToCSV, CSVContent.join('\n'));
    console.log(`Removed descriptive header for ${pathToCSV}!`)

    return lineArrs[0].reduce((acc: labelMap, curr, index) => {
        acc[curr] = lineArrs[1][index];
        return acc;
    }, {}) as labelMap;
}