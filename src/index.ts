const fs = require('fs');

const DB_FILE = 'test.cdb';

const createDBFile = (): void => {
    try {
        fs.writeFileSync('data/' + DB_FILE, '');
    } catch (err: unknown) {
        if (err instanceof Error) {
            throw new Error(`Error creating ${DB_FILE}: ${err.message}.`);
        } else {
            throw new Error('An error occured');
        }
    }
}

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

createDBFile();