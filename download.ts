import fs = require('fs');
import {getJSON, randomString} from './lib/util';

const downloadNeon = async () => {
    const rid = randomString(4);
    type dict = { [key: string]: string[]};
    let resCodes: dict = {};
    const sites = (await getJSON(`https://data.neonscience.org/api/v0/sites`)).data;
    for (const { siteCode, dataProducts } of sites) {
        for (const { dataProductTitle, dataProductCode, availableMonths } of dataProducts) {
            console.log(`Processing ${siteCode}, ${dataProductTitle}`);
            resCodes[dataProductCode] = dataProductTitle;
            break;
        }
        break;
    }
    
    fs.writeFileSync(`out/codes_${rid}.json`, JSON.stringify(resCodes, null, 4));
};

downloadNeon();