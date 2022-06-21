import Database from 'better-sqlite3';
import { Client } from 'pg';
import * as fs from 'fs';
import { importAllTables } from './import-sqlite'

// get command-line arguments
const args = process.argv.slice(2);
const configFile = args[0];
if (!configFile) {
    console.log('syntax: postgresql-datapump <configFile>');
    console.log('');
    console.log('configFile: json file with connection info');
    console.log(`    
    sqlite example:
    {
        "type": "sqlite",
        "input": "path/to/file.db",
    }
    csv example:
    {
        "type": "csv",
        "input": "path/to/file.csv",
    }
    `);
    process.exit(1);
}
// read contents of config file
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
        importAllTables(Database, Client, config);
        break;
    case 'csv':
        // importCsv(config);
        console.log('csv not implemented yet');
        break;
    default:
        console.log('config file must contain type (csv or sqlite)');
        process.exit(1);
}




