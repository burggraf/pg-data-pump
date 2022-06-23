export const quoteRow = (row: object) => {
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
export const getColumns = (row: object) => {
    if (row)
        return Object.keys(row).join(',');
    else return '';
}
