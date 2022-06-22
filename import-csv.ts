import { analyzeRowResults, analyzeRow } from './importHelpers';
import { connect as pgconnect } from './pg-client';
import Papa from 'papaparse';
import * as fs from 'fs';

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
    header: true,
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
  const schemaRows: any = await readCSV(config.input + '/' + filename, papaConfig);
  //console.log(schemaRows[0]);
  const fieldsHash = {};
  schemaRows.map((row) => {
      analyzeRow(fieldsHash, row);
    });  
  const fieldsArray = analyzeRowResults(fieldsHash);

  // remove extension from filename
  const table = filename.substring(0, filename.length - 4);

  let schema = 'CREATE TABLE IF NOT EXISTS "' + table + '" (\n';
  fieldsArray.map((field: any) => {
      schema += '  "' + field.sourceName + '" ' + field.type + ',\n';
      return null;
  })  
  // console.log('*****************************');
  // console.log('fieldsHash', fieldsHash);
  // console.log('*****************************');
  // console.log('fieldsArray', fieldsArray);
  // console.log('*****************************');
  schema = schema.substring(0, schema.length - 2) + '\n);\n';
  console.log('schema', schema);
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


