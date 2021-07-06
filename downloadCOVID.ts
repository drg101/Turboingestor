import fs = require('fs');
import csv = require('csv-parser');
import { getJSON, randomString, execCommand, downloadFile } from './lib/util';
import util = require('util');
const appendFile = util.promisify(fs.appendFile)

interface covidData { [fipsDate: string]: countyCovidData }
interface countyCovidData {
    GISJOIN: string,
    county: string,
    state: string,
    totalCaseCount?: number,
    totalDeathCount?: number,
    newCaseCount?: number,
    newDeathCount?: number,
    dateString: string,
    epoch_time: number
}

(async () => {
    const randomRun = randomString(4)
    const outFile = `./out/covid.csv`
    fs.openSync(outFile, 'w');
    fs.writeFileSync(outFile, '')
    fs.appendFileSync(outFile, 'GISJOIN,epoch_time,dateString,county,state,totalCaseCount,newCaseCount,totalDeathCount,newDeathCount\n');
    const filenameCases = `./out/covid_cases_raw_${randomRun}.csv`
    const filenameDeaths = `./out/covid_deaths_raw_${randomRun}.csv`
    const downloads = [
        downloadFile(filenameCases, `https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv`),
        downloadFile(filenameDeaths, `https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv`)
    ]
    await Promise.all(downloads)

    const nonDateFields = new Set("UID,iso2,iso3,code3,FIPS,Admin2,Province_State,Country_Region,Lat,Long_,Combined_Key,Population".split(","))

    let buffer: covidData = {};

    type covidType = "cases" | "deaths";
    const max = 7000;
    let i = 0;
    const addToBuffer = async (filename: string, fieldname: covidType) => {
        return new Promise<void>(resolve => {
            let writePromises: Promise<void>[] = [];
            fs.createReadStream(filename)
                .pipe(csv())
                .on('data', (data) => {
                    i++;
                    console.log(`${i/max*100}`.substr(0,3) + '%')
                    let previous = 0;
                    for (const [key, value] of Object.entries(data)) {
                        if (!nonDateFields.has(key)) {
                            //its a data field
                            const uniqueKey = `${data.FIPS}${key}`;
                            if (!buffer[uniqueKey]) {
                                const stringFips: string = `${data.FIPS}`;
                                buffer[uniqueKey] = {
                                    county: data.Admin2,
                                    state: data.Province_State,
                                    dateString: key,
                                    epoch_time: new Date(key).valueOf(),
                                    GISJOIN: `G${stringFips.length === 6 ? `0${stringFips.substr(0, 1)}` : stringFips.substr(0, 2)}0${stringFips.length === 6 ? stringFips.substr(1, 3) : stringFips.substr(2, 3)}0`
                                }
                            }
                            if (fieldname === "cases") {
                                buffer[uniqueKey].totalCaseCount = <number>value;
                                buffer[uniqueKey].newCaseCount = <number>value - previous;
                            }
                            else if (fieldname === "deaths") {
                                buffer[uniqueKey].totalDeathCount = <number>value;
                                buffer[uniqueKey].newDeathCount = <number>value - previous;
                            }

                            const { totalCaseCount, totalDeathCount } = buffer[uniqueKey]
                            if (totalCaseCount != null && totalDeathCount != null) {
                                (async () => {
                                    const copy = JSON.parse(JSON.stringify(buffer[uniqueKey]))
                                    writePromises.push(appendFile(outFile,
                                        `${copy.GISJOIN},${copy.epoch_time},${copy.dateString},${copy.county},${copy.state},${copy.totalCaseCount},${copy.newCaseCount},${copy.totalDeathCount},${copy.newDeathCount}\n`));
                                    delete buffer[uniqueKey]
                                })()
                                
                            }
                            previous = <number>value;
                        }
                    }
                })
                .on('end', async () => {
                    await Promise.all(writePromises)
                    resolve();
                });
        })
    }

    const toAdd = [
        addToBuffer(filenameCases, "cases"),
        addToBuffer(filenameDeaths, "deaths")
    ];

    await Promise.all(toAdd)
})()