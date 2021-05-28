import yargs = require('yargs/yargs');
import ingestCensus from './lib/ingestCensus';
import { init, done } from './lib/mongoBindings'


(async () => {
    await init();

    const { format, filepath, indexes } = await yargs(process.argv.slice(2)).array('indexes').options({
        format: { type: 'string', requiresArg: true, default: "census" },
        filepath: { type: 'string', requiresArg: true, demandOption: true },
        indexes: { type: 'array', requiresArg: true, default: [] }
    }).argv;



    switch (format) {
        case "census":
            ingestCensus(filepath, indexes);
            break;
    }

    done();
})();
