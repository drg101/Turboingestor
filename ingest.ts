import yargs = require('yargs/yargs');
import ingestCensus from './lib/ingestCensus';
import { exportLabelMap } from './lib/util' 


(async () => {

    const { format, filepath, indexes, name } = await yargs(process.argv.slice(2)).array('indexes').options({
        format: { type: 'string', requiresArg: true, default: "census" },
        filepath: { type: 'string', requiresArg: true, demandOption: true },
        indexes: { type: 'array', requiresArg: true, default: [] },
        name: { type: 'string', requiresArg: true, demandOption: true }
    }).argv;



    switch (format) {
        case "census_w_descriptive_header":
            const newFilePath = await exportLabelMap(filepath, name);
            ingestCensus(name, newFilePath, indexes);
            break;
        case "census":
            ingestCensus(name, filepath, indexes);
            break;
    }

})();
