import yargs = require('yargs/yargs');
import ingestCensus from './lib/ingestCensus';
import ingestNeon from './lib/ingestNeon';
import { exportLabelMap, exportLabelMapMulti, combineMultiyearCensusAndGetFilepath, normalizeCSVFiles, getMultiyearCensusFiles } from './lib/util' 



(async () => {

    const { format, filepath, indexes, name, table } = await yargs(process.argv.slice(2)).array('indexes').options({
        format: { type: 'string', requiresArg: true, default: "census" },
        filepath: { type: 'string', requiresArg: true, demandOption: true },
        indexes: { type: 'array', default: [] },
        name: { type: 'string', requiresArg: true, demandOption: true },
        table: { type: 'string' },
    }).argv;



    switch (format) {
        case "census_w_descriptive_header":
            const newFilePath = await exportLabelMap(filepath, name);
            ingestCensus(name, newFilePath, indexes);
            break;
        case "multiyear_census_w_descriptive_header":
            const files = getMultiyearCensusFiles(filepath)
            normalizeCSVFiles(files, name);
            //const newFilePaths = await exportLabelMapMulti(files, name);
            //const combinedFilePath = await combineMultiyearCensusAndGetFilepath(newFilePaths, name)
            //ingestCensus(name, combinedFilePath, ["GISJOIN", "epoch_time"]);
            break;
        case "census":
            ingestCensus(name, filepath, indexes);
            break;
        case "neon":
            table && ingestNeon(name, filepath, table);
            break;
    }

})();
