import fs = require('fs');
import { getJSON, randomString, execCommand } from './lib/util';
import chalk = require('chalk');

const downloadNeon = async () => {
    const rid = randomString(4);
    type dictArr = { [ key: string ]: string[] };
    type dict = { [ key: string ]: string };
    let resCodes: dict = {};
    const sites = (await getJSON(`https://data.neonscience.org/api/v0/sites`)).data;
    for (const { siteCode, dataProducts } of sites) {
        for (const { dataProductTitle, dataProductCode, availableMonths, availableDataUrls } of dataProducts) {
            console.log(`Processing ${siteCode}, ${dataProductTitle}`);
            resCodes[ dataProductCode ] = dataProductTitle;

            let packageDownloadsP: Promise<any>[] = [];
            for (const availableDataUrl of availableDataUrls) {
                packageDownloadsP.push((getJSON(availableDataUrl)));
            }
            const packageDownloads = await Promise.all(packageDownloadsP);
            console.log(`There are ${packageDownloads.length} packages which will be downloaded.`)

            let zipDownloadsP: Promise<boolean>[] = [];
            for (const { data } of packageDownloads) {
                const { month, packages } = data;
                const basicDownload = packages.find((pack: dict) => pack.type === 'basic');
                if(basicDownload){
                    const { url } = basicDownload;
                    console.log(`Downloading ${month} for ${siteCode}, ${dataProductTitle}`)
                    zipDownloadsP.push(execCommand(`wget -O ./out/downloads/${siteCode}_${dataProductCode}_${month}.zip ${url}`));
                }
            }
            await Promise.all(zipDownloadsP);
            console.log(chalk.green(`Finished downloading all zips for ${siteCode}, ${dataProductTitle}`))
            break;
        }
        break;
    }

    fs.writeFileSync(`out/codes_${rid}.json`, JSON.stringify(resCodes, null, 4));
};

downloadNeon();