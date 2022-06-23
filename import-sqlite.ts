import { analyzeRowResults, analyzeRow } from './importHelpers';
import { connect as pgconnect } from './pg-client';
import { connect as sqliteconnect } from './sqlite-client';
import { quoteRow, getColumns } from './utils';

let db: any
let client: any;

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

export const importAllTables = async (config: any) => {
    // get list of tables
    db = await sqliteconnect(config);
    client = await pgconnect(config);
    if (!db || !client) {
        process.exit(1);
    }
    const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((table: any) => table.name);
    while (tables.length > 0) {
        const tbl = tables.shift();
        if (typeof tbl === 'string' && (!config?.tables) || config?.tables.includes(tbl)) {
            console.log('importing table', tbl);
            await importTable(tbl);
        } else {
            console.log('skipping table not found in config.tables:', tbl);
        }
    }
    client.end();
    process.exit(0);
}
