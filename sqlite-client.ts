import Database from 'better-sqlite3';

export const connect = async (config: any) => {
    try {
        return new Database(config.input, {readonly: true, fileMustExist: true});
    } catch (err) {
        console.log('error connecting to SQLite database ' + config.input, err);
        return null;
    }
}
