import yargs = require('yargs/yargs');
import ingestCensus from './lib/ingestCensus';


(async () => {

    const { format, filepath, indexes, name } = await yargs(process.argv.slice(2)).array('indexes').options({
        format: { type: 'string', requiresArg: true, default: "census" },
        filepath: { type: 'string', requiresArg: true, demandOption: true },
        indexes: { type: 'array', requiresArg: true, default: [] },
        name: { type: 'string', requiresArg: true, demandOption: true }
    }).argv;



    switch (format) {
        case "census":
            ingestCensus(name, filepath, indexes);
            break;
    }

})();
