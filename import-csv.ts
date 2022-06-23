import { analyzeRowResults, analyzeRow } from './importHelpers';
import { connect as pgconnect } from './pg-client';
import Papa from 'papaparse';
import * as fs from 'fs';
import { quoteRow, getColumns } from './utils';

let client: any;

const readCSV = async (filePath: string, papaConfig: any) => {
  const csvFile = fs.readFileSync(filePath)
  const csvData = csvFile.toString()  
  return new Promise(resolve => {
    papaConfig.complete = (results: any) => {
      // console.log('Complete', results.data.length, 'records.'); 
      resolve(results.data);
    }
    Papa.parse(csvData, papaConfig);
  });
};

const parse_table = async (config: any, filename: string) => {
  client = await pgconnect(config);

  const papaConfig = {
    dynamicTyping: true,
    delimiter: "",	// auto-detect
    newline: "",	// auto-detect
    quoteChar: '"',
    escapeChar: '"',
    header: config.header,
    transformHeader: undefined,
    preview: 0,
    encoding: "",
    worker: true,
    comments: false,
    step: undefined,
    complete: undefined,
    error: undefined,
    download: false,
    downloadRequestHeaders: undefined,
    downloadRequestBody: undefined,
    skipEmptyLines: false,
    chunk: undefined,
    //chunkSize: 1024 * 1024 * 40,
    fastMode: undefined,
    beforeFirstChunk: undefined,
    withCredentials: undefined,
    transform: undefined,
    delimitersToGuess: [',', '\t', '|', ';', Papa.RECORD_SEP, Papa.UNIT_SEP]
  }
  //const file = fs.createReadStream(config.input + '/' + filename);
  //console.log('file', file);
  // parse file
  const allrows: any = await readCSV(config.input + '/' + filename, papaConfig);
  const fieldsHash = {};
  allrows.map((row) => {
      analyzeRow(fieldsHash, row);
    });  
  const fieldsArray = analyzeRowResults(fieldsHash);

  // remove extension from filename
  const table = filename.substring(0, filename.length - 4);

  let schema = 'CREATE TABLE IF NOT EXISTS "' + table + '" (\n';
  const headers = [];
  fieldsArray.map((field: any) => {
      let fieldname = field.sourceName;
      if (!config.header) fieldname = 'field' + fieldname;
      headers.push(fieldname);
      schema += '  "' + fieldname + '" ' + field.type + ',\n';
      return null;
  })  
  // console.log('*****************************');
  // console.log('fieldsHash', fieldsHash);
  // console.log('*****************************');
  // console.log('fieldsArray', fieldsArray);
  // console.log('*****************************');
  schema = schema.substring(0, schema.length - 2) + '\n);\n';
  console.log('schema', schema);

  try {
    console.log('create table:', table);
    const res = await client.query(schema);
  } catch (err) {
    console.log(err)
  }

  let index = 1;
  const columns = '"' + headers.join('","') + '"';;    
  const chunk = async (index: number, limit: number = 10000) => {
      const rows = allrows.slice(index, index + limit);

      //const rows = db.prepare(`select * from ${table} limit ${limit} offset ${index}`).all();
      const count = rows.length;
      console.log('count', count);
      if (rows.length === 0) {
          return count;
      }
      // console.log('rows', rows);
      for (let i = 0; i < rows.length; i++) {
          const row = rows[i];
          if (Object.keys(row).length !== headers.length) {
            console.log(`SKIPPING ROW WITH ${Object.keys(row).length} KEYS (EXPECTED ${headers.length})`);
            console.log(row);
            // remove row from array
            rows.splice(i, 1);
          }
      }
      const sql = `insert into "${table}" (${columns}) values 
          ${
              rows.map((row:object) => { 
                  return '(' + quoteRow(row) + ')';
              })
          };`;
        try {
          const res = await client.query(sql);
          return count;
        } catch (err) {
          console.log(err)
          return 0;
        }    
  }
  let count = await chunk(index, config.batch_size || 10000);
  while (count > 0) {
      index += count;
      count = await chunk(index, config.batch_size || 10000);
  }
  console.log('done', index);

  // Papa.parse(file, papaConfig);
  client.end();
  //process.exit(0);
  
}

export const importCSVFiles = async (config: any) => {
  // console.log(config);
  // { type: 'csv', input: '.', tables: [ 'AllStarFull.csv' ] }
  // get list of files in input directory
  while (config.tables.length > 0) {
    const filename = config.tables.shift();
    // get file object from filename
    parse_table(config, filename);
  }
}


