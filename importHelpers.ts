// write a function to test whether a string is a valid postgres timestamptz
export const isPostgresTimestamp = (str: any) => {
    if (typeof str !== 'string') return false;
    var match = str.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.\d+$/);
    if (match) {
      var year = match[1],
          month = match[2],
          day = match[3],
          hour = match[4],
          minute = match[5],
          second = match[6]
  
      if (year.length === 4 && month.length === 2 && day.length === 2 && hour.length === 2 && minute.length === 2 && second.length === 2) {
        return true;
      }
    }
    return false;
  }
  
  
  //write a function to test whether a string is a valid postgres datetime
export const isPostgresDateTime = (str: any) => {
    if (typeof str !== 'string') return false;
    var match = str.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
    if (match) {
      var year = match[1],
          month = match[2],
          day = match[3],
          hour = match[4],
          minute = match[5],
          second = match[6]
      if (year.length === 4 && month.length === 2 && day.length === 2 && hour.length === 2 && minute.length === 2 && second.length === 2) {
        return true;
      }
    }
    return false;
  }
  
  
  
export const isPostgresDate = (str: any) =>{
    if (typeof str !== 'string') return false;
    var match = str.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (match) {
      var year = match[1],
          month = match[2],
          day = match[3];
      if (year.length === 4 && month.length === 2 && day.length === 2) {
        return true;
      }
    }
    return false;
  }
  
  
  
export const analyzeRow = (fieldsHash: any, row: any) =>{
    for (let key in row) {
      const value = row[key]
      const field: any = fieldsHash[key] || (fieldsHash[key] = { typesFound: {}, sample: null, maxLength: 0, enabled: true })
  
      // Tally the presence of this field type
      const type = detectType(value)
      if (!field.typesFound[type]) field.typesFound[type] = 0
      field.typesFound[type]++
  
      // Save a sample record if there isn't one already (earlier rows might have an empty value)
      if (!field.sample && value) {
        field.sample = value
      }
  
      // Save the largest length
      if (typeof value === 'number') {
        field.maxLength = Math.max(field.maxLength, value)
      } else {
        field.maxLength = Math.max(field.maxLength, value!==null?value.length:0)
      }
    }
  }
  

export const detectType = (sample: any) =>{
  let sample2 = '';
  if (typeof sample === 'number') {sample2 = sample.toString();} else { sample2 = sample;}
  let retval = '';

  if (false && typeof sample !== 'string' && sample === '') {
      retval = 'null'
    } else if (isPostgresTimestamp(sample) && +sample >= 31536000) { 
      retval = 'timestamp'
    } else if (isPostgresDate(sample)) {
      retval = 'date'
    } else if (isPostgresDateTime(sample)) {
      retval = 'datetime'
    } else if (typeof sample === 'number' && !isNaN(sample) && sample2.includes('.')) {
      retval = 'numeric'
    } else if (sample === '1' || sample === '0' || (typeof sample === 'string' && sample.toLowerCase() === 'true') || (typeof sample === 'string' && sample.toLowerCase() === 'false')) {
      retval = 'boolean'
    } else if (typeof sample === 'number' && !isNaN(sample)) {
        try {
          // 4,294,967,295
          // if (sample.length > 9 && parseInt(sample.substr(0,1),10) > 3) {
          // if (parseInt(sample, 10) > 4294967295) {
          if (sample > 4294967295) {
            retval = 'bigint';
          } else {
            if (sample <= 32768 && sample >= -32768) {
              retval = 'smallint';
            } else {
              retval = 'integer';
            }
          }  
        } catch (e) {
          retval = 'bigint';
        }
    } else if (typeof sample === 'string' && sample.length > 255) {
      retval = 'text'
    } else if (sample === null) {
      retval = 'null'
    } else {
      retval = 'text' // string
    }
    // console.log('detectType', sample, typeof sample, 'retval:', retval);
    return retval;
  }
  
export const analyzeRowResults = (fieldsHash: any) => {
    let fieldsArray = []
    for (let key in fieldsHash) {
      const field = fieldsHash[key]
      // Determine which field type wins
      field.type = determineWinner(field.typesFound)
      field.machineName = key
      // field.machineName = slug(key, {
      //   replacement: '_',
      //   lower: true
      // })
      field.sourceName = key
      // If any null values encountered, set field nullable
      if (field.typesFound['null']) {
        field.nullable = true
      }
      fieldsArray.push(field)
    }
    return fieldsArray
  }
  
  /**
   *  Determine which type wins
   *  - timestamp could be int
   *  - integer could be float
   *  - everything could be string
   *  - if detect an int, don't check for timestamp anymore, only check for float or string
   *  - maybe this optimization can come later...
   */
export const determineWinner = (fieldTypes: any) =>{
    const keys = Object.keys(fieldTypes)
  
    if (keys.length === 1 && keys[0] !== 'null') {
      return keys[0]
    } else if (fieldTypes.text) {
      return 'text'
    } else if (fieldTypes.string) {
      return 'text'
    } else if (fieldTypes.numeric) {
      return 'numeric'
    } else if (fieldTypes.float) {
      return 'numeric'
    } else if (fieldTypes.integer) {
      return 'integer'
    } else if (fieldTypes.bigint) {
      return 'bigint'
    } else if (fieldTypes.smallint) {
      return 'smallint'
    } else if (fieldTypes.timestamp) {
      return 'timestamp'
    } else if (fieldTypes.datetime) {
      return 'datetime'
    } else if (fieldTypes.date) {
      return 'date'
    } else if (fieldTypes.boolean) {
        return 'boolean'
    } else { // TODO: if keys.length > 1 then... what? always string? what about date + datetime?
      // console.log('undetermined field type');
      // console.log('keys', keys);
      // console.log('fieldTypes', fieldTypes);
      if (fieldTypes[0] === 'null') return 'text'
      else return fieldTypes[0]
    }
  }
  
  