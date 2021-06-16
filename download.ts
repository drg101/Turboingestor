import fs = require('fs');
import { getJSON, randomString, execCommand } from './lib/util';
import chalk = require('chalk');
import yargs = require('yargs/yargs');
import { neonToken } from './lib/apiTokens'

type dictArr = { [ key: string ]: string[] };

interface resCode { 
    [key: string]: {
        title: string,
        sites: {}[], 
    }
};
type dict = { [ key: string ]: string };

const WAIT_TIME = 500;
let queueLen = 1;
const waitForTurn = async () => {
    return new Promise<void>(resolve => {
        setTimeout(() => {
            resolve();
            queueLen--;
        }, WAIT_TIME * queueLen)
        //console.log(chalk.red(WAIT_TIME*queueLen))
        queueLen++;
    });
}

const downloadAndUnzip = async ({month, siteCode, dataProductTitle, dataProductCode, url, out}: dict) => {
    console.log(`Downloading ${month} for ${siteCode}, ${dataProductTitle}`)
    const zipfile = `${out}/tmp/${siteCode}_${dataProductCode}_${month}.zip`
    await waitForTurn();
    await execCommand(`wget -O ${zipfile} ${url}?apiToken=${neonToken}`);
    const outDir = `${out}/${dataProductCode}/${siteCode}_${month}`;
    fs.mkdirSync(outDir);
    await execCommand(`unzip ${zipfile} -d ${outDir}`);
}

const downloadNeon = async (out: string) => {
    await execCommand(`rm -rf ${out}/*`);
    fs.mkdirSync(`${out}/tmp`);
    const rid = randomString(4);
    let resCodes: resCode = {};
    await waitForTurn();
    const sites = (await getJSON(`https://data.neonscience.org/api/v0/sites?apiToken=${neonToken}`)).data;
    for (const { siteCode, dataProducts } of sites) {
        for (const { dataProductTitle, dataProductCode, availableMonths, availableDataUrls } of dataProducts) {
            if (!fs.existsSync(`${out}/${dataProductCode}`)) {
                fs.mkdirSync(`${out}/${dataProductCode}`);
            }
            console.log(`Processing ${siteCode}, ${dataProductTitle}`);
            if(!resCodes[ dataProductCode ]){
                resCodes[ dataProductCode ] = {
                    title: dataProductTitle,
                    sites: [],
                };
            }
            fs.writeFileSync(`${out}/codes_${rid}.json`, JSON.stringify(resCodes, null, 4));

            const siteDescriptor = {
                code: siteCode,
                files: {} as dictArr
            };

            let packageDownloadsP: Promise<any>[] = [];
            for (const availableDataUrl of availableDataUrls) {
                await waitForTurn();
                packageDownloadsP.push((getJSON(availableDataUrl)));
            }
            const packageDownloads = await Promise.all(packageDownloadsP);
            console.log(`There are ${packageDownloads.length} packages which will be downloaded.`)

            let zipDownloadsP: Promise<void>[] = [];
            for (const { data } of packageDownloads) {
                const { month, packages, files } = data;
                siteDescriptor.files[`${siteCode}_${month}`] = files;
                const basicDownload = packages.find((pack: dict) => pack.type === 'basic');
                if (basicDownload) {
                    const { url } = basicDownload;
                    zipDownloadsP.push(downloadAndUnzip({month, siteCode, dataProductTitle, dataProductCode, url, out}));
                }
            }
            await Promise.all(zipDownloadsP);
            console.log(chalk.green(`Finished downloading all zips for ${siteCode}, ${dataProductTitle}`))
            resCodes[ dataProductCode ].sites.push(siteDescriptor);

            fs.writeFileSync(`${out}/codes_${rid}.json`, JSON.stringify(resCodes, null, 4));
        }
        break;
    }

    fs.writeFileSync(`${out}/codes_${rid}.json`, JSON.stringify(resCodes, null, 4));
};

(async () => {
    const { out } = await yargs(process.argv.slice(2)).array('indexes').options({
        out: { type: 'string', requiresArg: true, demandOption: true },
    }).argv;

    downloadNeon(out);
})()
