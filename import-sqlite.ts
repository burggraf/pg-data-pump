import { analyzeRowResults, analyzeRow } from './importHelpers';

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

let db;
let client;

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

const connectDB = async (Database: any, Client: any, config: any) => {
    db = new Database(config.input, {readonly: true, fileMustExist: true});
    client = new Client({ ssl: false });
    try {
        client.connect();
    } catch (err) {
        console.log('error connecting', err);
        process.exit(1);
    }
}
export const importAllTables = (Database: any, Client: any, config: any) => {
    // get list of tables
    connectDB(Database, Client, config);
    const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((table: any) => table.name);
    importTables(tables);
}
