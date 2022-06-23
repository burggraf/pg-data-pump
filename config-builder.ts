//import { prompt, Select } from 'enquirer';
const { Select, prompt, MultiSelect } = require('enquirer');
import { connect as sqliteconnect } from './sqlite-client';
import * as fs from 'fs';

export const configBuilder = async () => {
  console.log('*** (config-builder) configBuilder');
  let sqliteDB: any;
  let tables: string[];

  const p = new Select({
    name: 'type',
    message: 'Select import source:',
    choices: ['csv', 'sqlite']
  });
  const sourceType = await p.run();
  console.log('sourceType', sourceType);


  const inputResponse = await prompt({
    type: 'input',
    name: 'input',
    message: 'Input file name?',
    initial: './input.db',
  });
  console.log('response', inputResponse);

  if (sourceType === 'sqlite') {
    sqliteDB = await sqliteconnect({ input: inputResponse.input });
    if (!sqliteDB) process.exit(1);
  }

  const p2 = new Select({
    name: 'allTables',
    message: 'Include ALL tables?',
    choices: ['YES', 'NO']
  });
  const allTables = await p2.run();
  console.log('allTables', allTables);

  if (allTables === 'NO') {
    let tblList: any;
    const choices = [];
    try {
      tblList = sqliteDB.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((table: any) => table.name);
      tblList.map((tbl: string) => {
        choices.push({ name: tbl, value: tbl });
        return null;
      });
    } catch (err) {
      console.log('ERROR 2', err);
      process.exit(1);
    }
    const tablesResponse = new MultiSelect({
      name: 'value',
      message: 'Select tables to import',
      limit: 7,
      choices: choices
    });
    tables = await tablesResponse.run();
    console.log('tables', tables);
  }
  const batch_size = await prompt({
    type: 'input',
    name: 'batch_size',
    message: 'Batch size?',
    initial: '10000',
  });
  console.log('batch_size', batch_size);


  const configFilenameResponse = await prompt({
    type: 'input',
    name: 'configFilename',
    message: 'Name for this config file:',
    initial: 'config.cfg',
  });
  console.log('configFilenameResponse', configFilenameResponse);

  console.log("parseInt(batch_size.batch_size, 10)",parseInt(batch_size.batch_size, 10));
  if (isNaN(parseInt(batch_size.batch_size, 10))) {
    console.log(`INVALID BATCH SIZE: ${batch_size.batch_size}.  Using the default of 10000.`);
  }
  const config: any = {
    type: sourceType,
    input: inputResponse.input,
    batch_size: parseInt(batch_size.batch_size, 10) || 10000
  };
  if (tables) {
    config.tables = tables;
  }
  // write config file to disk
  fs.writeFileSync(configFilenameResponse.configFilename, JSON.stringify(config, null, 2));

  process.exit(0);
}

