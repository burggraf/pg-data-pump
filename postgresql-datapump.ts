import Database from 'better-sqlite3';
import { Client } from 'pg';
import { analyzeRowResults, analyzeRow } from './importHelpers';
import * as fs from 'fs';

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


const quoteRow = (row: object) => {
    const values = Object.values(row);
    for (let i = 0; i < values.length; i++) {
        if (typeof values[i] === 'string') {
            values[i] = `'${values[i].replace(/'/g, "''")}'`;
        } else if (values[i] === null) {
            values[i] = 'NULL';
        }
    }
    return values.join(',');
}
const getColumns = (row: object) => {
    if (row)
        return Object.keys(row).join(',');
    else return '';
}

const db = new Database('../boxball/boxball.db', {readonly: true, fileMustExist: true});
let client = new Client({ ssl: false });
try {
    client.connect();
} catch (err) {
    console.log('error connecting', err);
    process.exit(1);
}

const importTable = async (table: string) => {
    console.log('importing', table);

    const schemaRows = db.prepare(`select * from ${table} limit 100000`).all();
    const fieldsHash = {};
    schemaRows.map((row) => {
        analyzeRow(fieldsHash, row);
      });  
    const fieldsArray = analyzeRowResults(fieldsHash);
    let schema = 'CREATE TABLE IF NOT EXISTS ' + table + ' (\n';
    fieldsArray.map((field: any) => {
        schema += '  "' + field.sourceName + '" ' + field.type + ',\n';
        return null;
    })  
    schema = schema.substring(0, schema.length - 2) + '\n);\n';
    try {
        console.log('create table:', table);
        const res = await client.query(schema);
      } catch (err) {
        console.log(err)
      }

    let index = 0;
    
    const chunk = async (index: number, limit: number = 100000) => {
        const rows = db.prepare(`select * from ${table} limit ${limit} offset ${index}`).all();
        const count = rows.length;
        console.log('count', count);
        if (rows.length === 0) {
            return count;
        }
        const columns = getColumns(rows[0]);
        const sql = `insert into ${table} (${columns}) values 
            ${
                rows.map((row:object) => '(' + quoteRow(row) + ')')
            };`;
          try {
            const res = await client.query(sql);
            return count;
          } catch (err) {
            console.log(err)
            return 0;
          }    
    }
    let count = await chunk(index);
    while (count > 0) {
        index += count;
        count = await chunk(index);
    }
    console.log('done', index);
}

const importTables = async (tables: string[]) => {
    while (tables.length > 0) {
        const tbl = tables.shift();
        console.log('table', tbl, typeof tbl);
        if (typeof tbl === 'string') {
            await importTable(tbl);
        }
    }
    client.end();

}

// get list of tables
const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((table: any) => table.name);
importTables(tables);



