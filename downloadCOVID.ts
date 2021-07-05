import fs = require('fs');
import csv = require('csv-parser');
import { getJSON, randomString, execCommand, downloadFile } from './lib/util';

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

    const addToBuffer = async (filename: string, fieldname: covidType) => {
        return new Promise<void>(resolve => {
            fs.createReadStream(filename)
                .pipe(csv())
                .on('data', (data) => {
                    let previous = 0;
                    for (const [key, value] of Object.entries(data)) {
                        if (!nonDateFields.has(key)) {
                            //its a data field
                            if (!buffer[`${data.FIPS}${key}`]) {
                                buffer[`${data.FIPS}${key}`] = {
                                    county: data.Admin2;
                                }
                            }
                            previous = <number>value;
                        }
                    }
                })
                .on('end', () => {
                    resolve();
                });
        })
    }
})()