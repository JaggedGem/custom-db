import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const createTempDbFile = () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'custom-db-'));
    return path.join(dir, 'test.cdb');
};

const cleanupTempDbFile = (dbFilePath: string) => {
    const dir = path.dirname(dbFilePath);

    if (fs.existsSync(dbFilePath)) {
        fs.unlinkSync(dbFilePath);
    }

    fs.rmSync(dir, { recursive: true, force: true });
};

export { createTempDbFile, cleanupTempDbFile };
