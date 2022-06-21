import { analyzeRowResults, analyzeRow } from './importHelpers';
import { connect as pgconnect } from './pg-client';
import Papa from 'papaparse';

let client: any;

export const importCSVTables = async (config: any) => {
  config.importSpec = {  
    abort: false,
    ready: false,
    sourceURL: '',
    fieldNames: '',
    fieldTypes: [],
    destinationTable: '',
    quoteChar: '',
    DDL: '',
    status: 'ready',
    count: 0,
    processed: 0,
    errors: 0
  };

    client = await pgconnect(config);
    while (config.tables.length > 0) {
      const tablename = config.tables.shift();
      await importTable(config, tablename);
    }
    client.end();
}

const importCSV = async (importSpec, rows) => {

    console.log(`-> insert into ${importSpec.destinationTable}`);//, rows);
    // console.log('rows', rows);
    const { data, error} = await this.supabase.from(importSpec.destinationTable)
    .insert(rows, {returning: 'minimal'});
    if (error) {
      console.log('importCSV error', error);
    } else {
      importSpec.processed += rows.length;
      console.log(`processed ${importSpec.processed} / ${importSpec.count}`);
    }
    return { data, error };
  }


const analyzeFile = async (CHUNKSIZE) => {
    const fieldsHash = {};
    importSpec.status = 'analyzing';
    Papa.LocalChunkSize = CHUNKSIZE; // 1024 * 1024 * 10;	// 10 MB
    Papa.RemoteChunkSize = CHUNKSIZE; // 1024 * 1024 * 10;	// 5 MB
    const fileElement: any = document.getElementById('files');
    const file = fileElement.files[0];
    let rowCount = 0;
    let fieldsArray = [];
    console.log('calling parse with quoteChar', importSpec.quoteChar);
    await Papa.parse(file, {
      download: false, // true,
      // quoteChar: importSpec.enclosedBy,
      header: true, 
      transformHeader: importSpec.ready ? (header) => { return header; } : (header) => {
        // enclose fields names with quotes
        header = '"' + header.replace(/"/g,'') + '"';
        return header;
      },
      skipEmptyLines: true,
      quoteChar: importSpec.quoteChar,
      chunk: async function(results, parser) {
        if (importSpec.abort) parser.abort();
        if (importSpec.ready) { // do the actual data import / insert here
          parser.pause();
          const { data, error } = await importCSV(importSpec, results.data);
          if (error) {
            console.error('importCSV error', error);
          }
          console.log(`Records per sec: ${+((importSpec.processed / (+new Date() - timer) * 1000).toFixed(2))}`);
          console.log(`cursor ${results.meta.cursor} / ${(+new Date() - timer)}`);
          console.log(`Bytes per ms: ${+((results.meta.cursor / (+new Date() - timer)).toFixed(2))}`);
          parser.resume();
        } else { // analyze the file before importing
          console.log('**************************************');
          console.log("Row data.length:", results.data.length);
          console.log("Row errors.length:", results.errors.length);
          console.log('Chunk => Meta', results.meta);
          importSpec.count += results.data.length; 
          importSpec.errors += results.errors.length;
          results.data.map((row) => {
            if (rowCount > 0) analyzeRow(fieldsHash, row);
            rowCount++;
          });  
          if (results.errors.length > 0) {
            /*
            results.errors.map((error) => {
              if ((error.code === 'InvalidQuotes' || error.code === 'TooManyFields') && importSpec.quoteChar === '') {
              } else {
              }
            });
            */
            console.log(`*** there are ${results.errors.length} errors`);//, results.errors);
          }
        }
      },
      complete: function() {
        console.log('complete!');
        console.log('fieldsHash', fieldsHash);
        console.log('fieldNames is now', importSpec.fieldNames);
        console.log('record count', importSpec.count);
        if (importSpec.ready) { // import should be done here
          console.log('READY -> complete function skipped, we should be done.');
          const totalTime = +new Date() - timer;
          console.log(`TOTAL TIME: ${totalTime}`);
          console.log(`Records per sec: ${+((importSpec.processed / (+new Date() - timer) * 1000).toFixed(2))}`);
          return;
        } // still analyzing the file here
        const fieldsArray = analyzeRowResults(fieldsHash);
        console.log('**************************************************');
        console.log('fieldsArray', fieldsArray);
        console.log('**************************************************');
        const assignedFieldNames = [];
        let DDL = `(`;
        for (let x = 0; x < fieldsArray.length; x++) {
          let fieldName = fieldsArray[x].sourceName || 'field'.trim();
          if (assignedFieldNames.indexOf(fieldName) > -1) {
            let suffix = 1;
            while (assignedFieldNames.indexOf(fieldName + suffix) > -1) {
              suffix++;
            }
            fieldName += suffix;
          }
          DDL += `${fieldName} ${fieldsArray[x].type.toUpperCase()}`;
          assignedFieldNames.push(fieldName.trim());
          importSpec.fieldTypes.push(fieldsArray[x].type.toUpperCase());
          if (x < fieldsArray.length - 1) DDL += `,\n `;
        }
        DDL += `)`;        
        console.log('DDL', DDL);
        importSpec.DDL = `CREATE TABLE "${importSpec.destinationTable}"\n${DDL}`;
        importSpec.fieldNames = assignedFieldNames.join('\t');
        importSpec.status = 'analyzed';
        checkDestinationTable();
      }
    });
  }

const importTable = async (config: any, table: string) => {
      config.importSpec.count = 0;
      config.importSpec.errors = 0;
      config.importSpec.processed = 0;
      await analyzeFile(1024 * 1024 * 20); // 20MB chunk size for analyzing the file
      
}