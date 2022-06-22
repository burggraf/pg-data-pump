import * as fs from 'fs';

import { configBuilder } from './config-builder';
import { importAllTables } from './import-sqlite'
import { importCSVFiles } from './import-csv';

// get command-line arguments
const args = process.argv.slice(2);
const configFile = args[0];


const mainloop = async (configFile: any) => {
    let config;
    try {
        config = JSON.parse(fs.readFileSync(configFile, 'utf8'));
    } catch (err: any) {
        console.log('error reading config file: ', configFile);
        console.log('(must be a valid json file)');
        process.exit(1);
    }
    if (!config?.type) {
        console.log('config file must contain type (csv or sqlite)');
        process.exit(1);
    }
    if (!config?.input) {
        console.log('config file must contain input (path to input file)');
        process.exit(1);
    }
    
    switch (config.type) {
        case 'sqlite':
            importAllTables(config);
            break;
        case 'csv':
            // importCsv(config);
            importCSVFiles(config);
            break;
        default:
            console.log('config file must contain type (csv or sqlite)');
            process.exit(1);
    }
    
}


const checkConfigFile = async () => {
    console.log('*** checkConfigFile');
    const args = process.argv.slice(2);
    const configFile = args[0];    
    console.log('configFile', configFile);
    if (!configFile) {
        await configBuilder(); 
     }
     else {
        mainloop(configFile);
     }
}
checkConfigFile();


