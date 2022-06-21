import { Client } from 'pg';

export const connect = async (config: any) => {
    const client = new Client({ ssl: false });
    try {
        client.connect();
        return client;
    } catch (err) {
        console.log('error connecting to PostgreSQL database', err);
        return null;
    }
}

export default Client;