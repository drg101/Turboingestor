import yargs = require('yargs/yargs');


(async () => {
    const argv = await yargs(process.argv.slice(2)).options({
        format: { type: 'string' },
    }).argv;

    switch(argv.format){
        case "census":
            break;
    }
})();
