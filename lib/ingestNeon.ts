import fs = require('fs');
import { randomString } from './util'

const ingestNeon = async (name: string, filepath: string) => {
    console.log(`Neon manifest is ${filepath}`)
    const manifest = JSON.parse(fs.readFileSync(filepath, 'utf8'));
    const { siteCodes, productCode, productName, releases } = manifest;
    console.log(`Sitecodes are: ${siteCodes}`)
    const uniqueRun = randomString(4);
    console.log(`Unique code for this run is ${uniqueRun}`)

    const timeSeriesFileName = `${name}_time_series_${uniqueRun}.csv`
    console.log(`Creating Time Series: ${timeSeriesFileName}`)
    fs.openSync(`./out/${timeSeriesFileName}`, 'w');

    const labelMapFileName = `${name}_label_map_${uniqueRun}.json`
    console.log(`Creating Label map: ${labelMapFileName}`)
    fs.openSync(`./out/${labelMapFileName}`, 'w');

    const locationsFileName = `${name}_locations_${uniqueRun}.csv`
    console.log(`Creating Locations: ${locationsFileName}`)
    fs.openSync(`./out/${locationsFileName}`, 'w');

    for(const siteCode of siteCodes){
        let relevantPackages = [];
        for(const release of releases) {
            for(const releasePackage of release.packages){
                if(releasePackage.siteCode === siteCode){
                    relevantPackages.push(releasePackage);
                }
            }
        }
        console.log(relevantPackages)
        break;
    }
}

export default ingestNeon;