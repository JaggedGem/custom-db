const fs = require('fs');

const DB_FILE = 'test.cdb';

const readDBFile = (): string => {
    try {
        const data = fs.readFileSync('data/' + DB_FILE, 'utf-8');

        return data;
    } catch (err: unknown) {
        if (err instanceof Error) {
            throw new Error(`No file called ${DB_FILE} exists in the data folder.`);
        } else {
            throw new Error('An error occured');
        }
    }
}

const writeDBFile = (content: string) => {
    try {
        fs.appendFileSync('data/' + DB_FILE, '\n' + content)
    } catch (err: unknown) {
        if (err instanceof Error) {
            throw new Error('Error: ' + err.message);
        } else {
            throw new Error('An error occured');
        }
    }
}

console.log('File data: \n' + readDBFile());
writeDBFile('test123');
console.log('\nFile data: \n' + readDBFile());