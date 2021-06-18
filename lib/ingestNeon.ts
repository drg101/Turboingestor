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
const ingestNeon = async (name: string, filepath: string, tableName: string) => {
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
    let locationsGeojsonList: dictionaryAny[] = [];

    let timeSeriesHeaderIsDone = false;
    let labelMapIsDone = false;
    for (const site of [...new Set(files.map(file => file.site))]) {
        const siteID = `${site}_${productCode}`;
        console.log(siteID);

        let locationsForSiteCode: string[] = [];
        let geojsonPositionsForSiteCode: dictionaryAny[] = [];
        let locsWhichAreUsed: Set<string> = new Set();
        for (const { month, fileName } of files.filter(file => file.site === site)) {
            //console.log(`Package folder is ${fileName}`)
            const innerFileList = fs.readdirSync(`${filepath}/${fileName}`)
            if (!locationsForSiteCode.length) {
                console.log(`Getting sensor pos for ${siteID}`)
                for (const file of innerFileList) {
                    if (file.match(/^.*sensor_positions.*.csv$/g)) {
                        console.log(`File with positions is: ${file}`)
                        const positions = await new Promise<dictionaryAny[]>(resolve => {
                            let datum: dictionaryAny[] = [];
                            const posStream = fs.createReadStream(`${filepath}/${fileName}/${file}`)
                                .pipe(csv())
                                .on('data', (data) => {
                                    data.loc = data['HOR.VER'];
                                    datum.push(data);
                                })
                                .on('end', () => { resolve(datum) })
                        });

                        const geojsonPositions = positions.map(position => {
                            const site = `${siteID}_${position?.loc}`
                            return {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [Number(position?.referenceLongitude), Number(position?.referenceLatitude)]
                                },
                                "properties": {
                                    ...position,
                                    site,
                                    collectionName: name,
                                    name: position?.name
                                },
                                site
                            }
                        });

                        geojsonPositionsForSiteCode.push(...geojsonPositions);
                        locationsForSiteCode.push(...positions.map(position => position.loc))
                        break;
                    }
                }

            }

            if (!labelMapIsDone) {
                console.log(`Buidling label map for ${name}`)
                let foundLabels = false;
                for (const file of innerFileList) {
                    if (file.match(/^.*variables.*\.csv$/g)) {
                        console.log(`File with mapping is: ${file}`)
                        let mapping: {}[] = [];
                        mapping.push({
                            "name": "epoch_time",
                            "label": "Date",
                            "type": "date",
                            "step": "day"
                        });
                        await new Promise<void>(resolve => {
                            fs.createReadStream(`${filepath}/${fileName}/${file}`)
                                .pipe(csv())
                                .on('data', ({ table, fieldName, description, units }) => {
                                    if (table === tableName) {
                                        foundLabels = true;
                                        mapping.push({
                                            name: fieldName,
                                            label: description,
                                            unit: units,
                                            hideByDefault: true
                                        })
                                    }
                                })
                                .on('end', () => { resolve() })
                        });
                        fs.writeFileSync(`./out/${labelMapFileName}`, JSON.stringify({
                            collection: name,
                            icon: "science",
                            temporal: "epoch_time",
                            linked: {
                                collection: "neon_sites",
                                field: "site"
                            },
                            fieldMetadata: mapping
                        }, null, 4));
                        break;
                    }
                }
                labelMapIsDone = foundLabels;
            }

            const tsRegex = new RegExp(`^.*${tableName}.*$`)
            let tsFiles: string[] = [];
            for (const file of innerFileList) {
                if (file.match(tsRegex)) {
                    tsFiles.push(file);
                    console.log(`Time series file is ${file}`)
                }
            }


            if (!timeSeriesHeaderIsDone && tsFiles.length) {
                console.log(`Adding time series header to out!`)
                const fileStream = fs.createReadStream(`${filepath}/${fileName}/${tsFiles[0]}`);
                const rl = readline.createInterface({
                    input: fileStream,
                    crlfDelay: Infinity
                });
                for await (const line of rl) {
                    fs.appendFileSync(`./out/${timeSeriesFileName}`, `${line}\n`);
                    break;
                }
                timeSeriesHeaderIsDone = true;
            }

            for (const tsFile of tsFiles) {
                const locationAtSite = locationsForSiteCode.find(loc => { 
                    const matchStr = `^.*${loc.replace(".","\.")}\..*\.${tableName}.*$`
                    return tsFile.match(new RegExp(matchStr)) 
                });

                if(!locationAtSite){
                    throw "bad"
                }
                locsWhichAreUsed.add(locationAtSite);

                const site = `${siteID}_${locationAtSite}`
                console.log(`Collection epoch times from ${tsFile}`)
                let times: number[] = [];
                await new Promise<void>(resolve => {
                    fs.createReadStream(`${filepath}/${fileName}/${tsFile}`)
                        .pipe(csv())
                        .on('data', (data) => { times.push(new Date(data.startDateTime).valueOf()) })
                        .on('end', () => { resolve() })
                });
                console.log(`Pasting time series for ${tsFile}`)
                const fileStream = fs.createReadStream(`${filepath}/${fileName}/${tsFile}`);
                const rl = readline.createInterface({
                    input: fileStream,
                    crlfDelay: Infinity
                });
                let index = 0;
                for await (const line of rl) {
                    if (index !== 0) {
                        const time = times.shift();
                        !line.split(',').includes('') && fs.appendFileSync(`./out/${timeSeriesFileName}`, `${site},${time},${line}\n`);
                    }
                    index++;
                }
            }
        }
        locationsGeojsonList.push(...geojsonPositionsForSiteCode.filter(location => locsWhichAreUsed.has(location.properties.loc)));
        fs.writeFileSync(`./out/${locationsFileName}`, JSON.stringify(locationsGeojsonList, null, 4));
    }

    console.log(`Importing ${name} locations.`)
    fs.writeFileSync(`./out/${locationsFileName}`, JSON.stringify(locationsGeojsonList, null, 4));
    await appendJSONList('neon_sites', `./out/${locationsFileName}`);

    console.log(`Importing ${name} time series.`)
    await importCSV(name, `./out/${timeSeriesFileName}`);

    console.log(`Creating indexes for ${name}`)
    await createIndexes(name, ['site', 'epoch_time']);
}

export default ingestNeon;