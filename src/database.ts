import * as fs from 'fs';
import {
    PAGE_SIZE,
    NEXT_PAGE_ID_POSITION,
    NEXT_SLOT_OFFSET_POSITION,
    RECORD_COUNT_POSITION,
    PAGE_TYPES,
    PAGE_TYPE_POSITION,
} from './constants';
import { readPage } from './page';
import { StorageError, StorageErrorCode } from './errors';

// todo: remove the static master header writing
const initDatabase = (filePath: string, overwrite: boolean = false) => {
    const fd = fs.openSync(filePath, overwrite ? 'w+' : 'a+');

    const stats = fs.fstatSync(fd);
    if (stats.size === 0 || overwrite) {
        const header = Buffer.alloc(PAGE_SIZE);

        // writing identificator for file type
        header.write('CDB', 0, 'utf8');

        // writing the version (v1)
        header.writeUInt8(1, 4);

        // writing the page size (4096) from offset
        header.writeUInt16LE(PAGE_SIZE, 5);

        // Next Free Page ID = 2
        // (page 0 is the header & page 1 are the table definitions so the next free page id is 2)
        header.writeUInt32LE(2, 7);

        // save data to disk
        const written = fs.writeSync(fd, header, 0, PAGE_SIZE, 0);
        if (written !== PAGE_SIZE) {
            throw new StorageError(
                StorageErrorCode.SHORT_WRITE,
                'Short Write while writing MASTER header',
                {
                    context: {
                        pageId: 0,
                        expectedBytes: PAGE_SIZE,
                        actualBytes: written,
                        position: 0,
                    },
                },
            );
        }

        const tableDefs = Buffer.alloc(PAGE_SIZE);
        tableDefs.writeUInt32LE(0, NEXT_PAGE_ID_POSITION); // default next page id = 0 (no other page exists for now)
        tableDefs.writeUInt16LE(16, NEXT_SLOT_OFFSET_POSITION); // default next offset = 16 (header size = 16 bytes)
        tableDefs.writeUInt16LE(0, RECORD_COUNT_POSITION); // default number of tables = 0
        // empty padding (7 bytes)
        tableDefs.writeUInt8(PAGE_TYPES.CATALOG_TABLE, PAGE_TYPE_POSITION); // refer to PAGE_TYPES

        const written1 = fs.writeSync(fd, tableDefs, 0, PAGE_SIZE, PAGE_SIZE);
        if (written1 !== PAGE_SIZE) {
            throw new StorageError(
                StorageErrorCode.SHORT_WRITE,
                'Short Write while writing table definitions page header',
                {
                    context: {
                        pageId: 1,
                        expectedBytes: PAGE_SIZE,
                        actualBytes: written1,
                        position: PAGE_SIZE,
                    },
                },
            );
        }

        fs.fsyncSync(fd);
    } else {
        const { page: header } = readPage(fd, 0, 'initDatabase');

        if (header.toString('utf8', 0, 3) !== 'CDB') {
            throw new StorageError(
                StorageErrorCode.IO_ERROR,
                'Invalid database file (bad magic)',
            );
        }

        const version = header.readUInt8(4);
        if (version !== 1) {
            throw new StorageError(
                StorageErrorCode.IO_ERROR,
                `Unsupported DB version: ${version}`,
            );
        }

        const pageSize = header.readUInt16LE(5);
        if (pageSize !== PAGE_SIZE) {
            throw new StorageError(
                StorageErrorCode.IO_ERROR,
                'Page size mismatch',
                {
                    context: {
                        expectedPageSize: PAGE_SIZE,
                        actualPageSize: pageSize,
                    },
                },
            );
        }
    }

    return fd;
};

const closeDatabase = (fd: number) => {
    fs.fsyncSync(fd);
    fs.closeSync(fd);
};

export { initDatabase, closeDatabase };
