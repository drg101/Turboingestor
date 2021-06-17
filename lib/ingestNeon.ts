import fs = require('fs');
import { randomString } from './util'
import csv = require('csv-parser');
import readline = require('readline');
import { importCSV, importJSON, appendJSONList, createIndexes } from './mongoBindings';
import path = require('path')

type dictionary = {
    [key: string]: string
}

type dictionaryAny = {
    [key: string]: any
}

//welp, atleast theres not really any external dependencies.
const ingestNeon = async (name: string, filepath: string) => {
    const tableName = 'BP_30min'
    let files: dictionary[] = fs.readdirSync(filepath).map(fileName => {
        return {
            site: fileName.split('_')[0],
            month: fileName.split('_')[1],
            fileName
        }
    });
    files = files.sort((a, b) => new Date(a.month).valueOf() - new Date(b.month).valueOf());
    const productCode = path.basename(filepath)

    const findFileMatch = (regexp: RegExp, fileList = files.map(file => file.fileName)) => {
        for (const fileName of fileList) {
            if (fileName.match(regexp)) {
                return fileName;
            }
        }
    }

    const uniqueRun = randomString(4);
    console.log(`Unique code for this run is ${uniqueRun}`)

    const timeSeriesFileName = `${name}_time_series_${uniqueRun}.csv`
    console.log(`Creating Time Series: ${timeSeriesFileName}`)
    fs.appendFileSync(`./out/${timeSeriesFileName}`, 'site,epoch_time,');

    const labelMapFileName = `${name}_label_map_${uniqueRun}.json`
    console.log(`Creating Label map: ${labelMapFileName}`)
    fs.openSync(`./out/${labelMapFileName}`, 'w');

    const locationsFileName = `${name}_locations_${uniqueRun}.json`
    console.log(`Creating Locations: ${locationsFileName}`)
    fs.openSync(`./out/${locationsFileName}`, 'w');
    let locationsGeojsonList: {}[] = [];

    let timeSeriesHeaderIsDone = false;
    let labelMapIsDone = false;
    for (const site of [...new Set(files.map(file => file.site))]) {
        const siteID = `${site}_${productCode}`;
        console.log(siteID);

        let locationsForSiteCode = [];
        for (const { month, fileName } of files.filter(file => file.site === site)) {
            //console.log(`Package folder is ${fileName}`)

            if (!locationsForSiteCode.length) {
                console.log(`Getting sensor pos for ${siteID}`)
                for (const file of fs.readdirSync(`${filepath}/${fileName}`)) {
                    if (file.match(/^.*sensor_positions.*.csv$/g)) {
                        console.log(`File with positions is: ${file}`)
                        const positions = await new Promise(resolve => {
                            let datum: dictionaryAny[] = [];
                            const posStream = fs.createReadStream(`${filepath}/${fileName}/${file}`)
                                .pipe(csv())
                                .on('data', (data) => { datum.push(data); })
                                .on('end', () => { resolve(datum) })
                        });
                        console.log(positions)

                        // locationsGeojsonList.push({
                        //     "type": "Feature",
                        //     "geometry": {
                        //         "type": "Point",
                        //         "coordinates": [ Number(referenceLongitude), Number(referenceLatitude) ]
                        //     },
                        //     "properties": {
                        //         site: siteID,
                        //         name,
                        //     },
                        //     "site": siteID
                        // });
                        // break;
                    }
                }
                locationsForSiteCode.push('poop')
            }

            //     if (!labelMapIsDone) {
            //         console.log(`Buidling label map for ${name}`)
            //         for (const file of relevantPackage.files) {
            //             if (file.fileName.match(/^.*variables.*\.csv$/g)) {
            //                 console.log(`File with mapping is: ${file.fileName}`)
            //                 let mapping: {}[] = [];
            //                 await new Promise<void>(resolve => {
            //                     fs.createReadStream(`${filepath}/${folder}/${file.fileName}`)
            //                         .pipe(csv())
            //                         .on('data', ({ table, fieldName, description, units }) => {
            //                             if (table === tableName) {
            //                                 mapping.push({
            //                                     name: fieldName,
            //                                     label: description,
            //                                     unit: units
            //                                 })
            //                             }
            //                         })
            //                         .on('end', () => { resolve() })
            //                 });
            //                 fs.writeFileSync(`./out/${labelMapFileName}`, JSON.stringify(mapping, null, 4));
            //                 break;
            //             }
            //         }
            //         labelMapIsDone = true;
            //     }

            //     const tsRegex = new RegExp(`^.*${tableName}.*$`)
            //     let tsFile: string = "";
            //     for (const file of relevantPackage.files) {
            //         if (file.fileName.match(tsRegex)) {
            //             tsFile = file.fileName;
            //             console.log(`Time series file is ${tsFile}`)
            //             break;
            //         }
            //     }

            //     if (!timeSeriesHeaderIsDone) {
            //         console.log(`Adding time series header to out!`)
            //         const fileStream = fs.createReadStream(`${filepath}/${folder}/${tsFile}`);
            //         const rl = readline.createInterface({
            //             input: fileStream,
            //             crlfDelay: Infinity
            //         });
            //         for await (const line of rl) {
            //             fs.appendFileSync(`./out/${timeSeriesFileName}`, `${line}\n`);
            //             break;
            //         }
            //         timeSeriesHeaderIsDone = true;
            //     }

            //     console.log(`Collection epoch times from ${folder}`)
            //     let times: number[] = [];
            //     await new Promise<void>(resolve => {
            //         fs.createReadStream(`${filepath}/${folder}/${tsFile}`)
            //             .pipe(csv())
            //             .on('data', (data) => { times.push(new Date(data.startDateTime).valueOf()) })
            //             .on('end', () => { resolve() })
            //     });

            //     console.log(`Pasting time series for ${folder}`)
            //     const fileStream = fs.createReadStream(`${filepath}/${folder}/${tsFile}`);
            //     const rl = readline.createInterface({
            //         input: fileStream,
            //         crlfDelay: Infinity
            //     });
            //     let index = 0;
            //     for await (const line of rl) {
            //         if (index !== 0) {
            //             const time = times.shift();
            //             !line.split(',').includes('') && fs.appendFileSync(`./out/${timeSeriesFileName}`, `${siteID},${time},${line}\n`);
            //         }
            //         index++;
            //     }
        }
    }

    // console.log(`Importing ${name} locations.`)
    // fs.writeFileSync(`./out/${locationsFileName}`, JSON.stringify(locationsGeojsonList, null, 4));
    // await appendJSONList('neon_sites', `./out/${locationsFileName}`);

    // console.log(`Importing ${name} time series.`)
    // await importCSV(name, `./out/${timeSeriesFileName}`);

    // console.log(`Creating indexes for ${name}`)
    // await createIndexes(name, ['site', 'epoch_time']);
}

export default ingestNeon;