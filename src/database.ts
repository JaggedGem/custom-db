import * as fs from 'fs';
import {
    PAGE_SIZE,
    PAGE_TYPES,
    MH_IDENTIFICATOR_POSITION,
    MH_NEXT_FREE_PAGE_ID_POSITION,
    MH_PAGE_SIZE_POSITION,
    MH_VERSION_POSITION,
} from './constants';
import { allocatePage, readPage } from './page';
import { StorageError, StorageErrorCode } from './errors';

const initDatabase = (filePath: string, overwrite: boolean = false) => {
    const fd = fs.openSync(filePath, overwrite ? 'w+' : 'a+');

    const stats = fs.fstatSync(fd);
    if (stats.size === 0 || overwrite) {
        const header = Buffer.alloc(PAGE_SIZE);

        // writing identificator for file type
        header.write('CDB', MH_IDENTIFICATOR_POSITION, 'utf8');

        // writing the version (v1)
        header.writeUInt8(1, MH_VERSION_POSITION);

        // writing the page size (4096) from offset
        header.writeUInt16LE(PAGE_SIZE, MH_PAGE_SIZE_POSITION);

        // Next Free Page ID = 1
        // (page 0 is the header) this will be immediately bumped to 2 because of the creation of the table definitions page
        header.writeUInt32LE(1, MH_NEXT_FREE_PAGE_ID_POSITION);

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

        allocatePage(fd, PAGE_TYPES.CATALOG_TABLE, 'initDatabase');
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
