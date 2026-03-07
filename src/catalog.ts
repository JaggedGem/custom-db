import * as fs from 'fs';
import { allocatePage, readPage, writeHeader, getLatestPage } from './page';
import {
    PAGE_SIZE,
    NEXT_PAGE_ID_POSITION,
    NEXT_SLOT_OFFSET_POSITION,
    RECORD_COUNT_POSITION,
    DATA_TYPES,
    PAGE_TYPES,
    TABLE_SLOT_SIZE,
    COLUMN_SLOT_SIZE,
} from './constants';
import {
    StorageErrorCode,
    StorageError,
    ValidationErrorCode,
    ValidationError,
} from './errors';
import {
    createBitmapPage,
    createFixedPage,
    createSlottedPage,
} from './data-pages';
import { Column, ForeignKeyColumn, DatabaseContext, Table } from './types';

const isForeignKey = (col: Column): col is ForeignKeyColumn =>
    col.isForeignKey === true;

const createColumn = (fd: number, startingPageId: number, column: Column) => {
    if (Buffer.byteLength(column.name, 'utf8') > 12) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'Column name should be smaller than 12 bytes',
            {
                context: {
                    expectedValue: '<12',
                    actualValue: Buffer.byteLength(column.name, 'utf8'),
                },
            },
        );
    }

    // find the last page (to add new columns)
    let pageId = getLatestPage(fd, startingPageId, PAGE_TYPES.CATALOG_COLUMN);
    let colDefs = readPage(fd, pageId, 'createColumn');

    let nextOffset = colDefs.nextOffset;
    let nrColumns = colDefs.recordCount;

    // check if the page has space for a new 48-byte definition
    if (nextOffset + COLUMN_SLOT_SIZE > PAGE_SIZE) {
        const newPageId = allocatePage(
            fd,
            PAGE_TYPES.CATALOG_COLUMN,
            'createColumn',
        );

        // link the current page to the new one
        colDefs.page.writeUInt32LE(newPageId, NEXT_PAGE_ID_POSITION);
        const written = fs.writeSync(
            fd,
            colDefs.page,
            0,
            PAGE_SIZE,
            pageId * PAGE_SIZE,
        );
        if (written !== PAGE_SIZE) {
            throw new StorageError(
                StorageErrorCode.SHORT_WRITE,
                'Short Write while linking column definitions page',
                {
                    context: {
                        pageId,
                        expectedBytes: PAGE_SIZE,
                        actualBytes: written,
                        position: pageId * PAGE_SIZE,
                    },
                },
            );
        }

        // create the header for the new definitions page
        colDefs.page = writeHeader(
            fd,
            newPageId,
            PAGE_TYPES.CATALOG_COLUMN,
            'createColumn',
        );
        pageId = newPageId;

        nextOffset = 16;
        nrColumns = 0;
    }

    // set column name (12 bytes)
    colDefs.page.fill(0, nextOffset, nextOffset + 12); // clean before write
    colDefs.page.write(column.name, nextOffset, 'utf8');

    // set the type of data (at offset 12 from slot start)
    const typeOffset = nextOffset + 12;
    let columnDataId = 0;

    if (!isForeignKey(column)) {
        if (column.type === 'boolean') {
            columnDataId = createBitmapPage(fd);
            colDefs.page.writeUInt8(DATA_TYPES.BOOLEAN, typeOffset);
        } else if (column.type === 'integer') {
            columnDataId = createFixedPage(fd);
            colDefs.page.writeUInt8(DATA_TYPES.INTEGER, typeOffset);
        } else if (column.type === 'string') {
            columnDataId = createSlottedPage(fd);
            colDefs.page.writeUInt8(DATA_TYPES.STRING, typeOffset);
        }
    } else {
        colDefs.page.writeUInt8(DATA_TYPES.FOREIGN_KEY, typeOffset);

        if (Buffer.byteLength(column.foreignKey.table, 'utf8') > 12) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'Foreign key table name should be smaller than 12 bytes',
                {
                    context: {
                        expectedValue: '<12',
                        actualValue: Buffer.byteLength(
                            column.foreignKey.table,
                            'utf8',
                        ),
                    },
                },
            );
        }
        if (Buffer.byteLength(column.foreignKey.column, 'utf8') > 12) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'Foreign key column name should be smaller than 12 bytes',
                {
                    context: {
                        expectedValue: '<12',
                        actualValue: Buffer.byteLength(
                            column.foreignKey.column,
                            'utf8',
                        ),
                    },
                },
            );
        }

        columnDataId = createFixedPage(fd);

        // target table (offset 17 from slot start)
        colDefs.page.fill(0, typeOffset + 5, typeOffset + 17);
        colDefs.page.write(column.foreignKey.table, typeOffset + 5, 'utf8');

        // target column (offset 29 from slot start)
        colDefs.page.fill(0, typeOffset + 17, typeOffset + 29);
        colDefs.page.write(column.foreignKey.column, typeOffset + 17, 'utf8');
    }

    // set the starting page for the actual data (at offset 13 from slot start)
    colDefs.page.writeUInt32LE(columnDataId, nextOffset + 13);

    // update metadata for this page
    colDefs.page.writeUInt16LE(
        nextOffset + COLUMN_SLOT_SIZE,
        NEXT_SLOT_OFFSET_POSITION,
    );
    colDefs.page.writeUInt16LE(nrColumns + 1, RECORD_COUNT_POSITION);

    const written = fs.writeSync(
        fd,
        colDefs.page,
        0,
        PAGE_SIZE,
        pageId * PAGE_SIZE,
    );
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing column definition',
            {
                context: {
                    pageId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written,
                    position: pageId * PAGE_SIZE,
                },
            },
        );
    }
    fs.fsyncSync(fd);

    return columnDataId;
};

const createTable = (fd: number, name: string, columns: Column[]) => {
    if (Buffer.byteLength(name, 'utf8') > 12) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'Table name should be smaller than 12 bytes',
            {
                context: {
                    expectedValue: '<12',
                    actualValue: Buffer.byteLength(name, 'utf8'),
                },
            },
        );
    }

    let pageId = getLatestPage(fd, 1, PAGE_TYPES.CATALOG_TABLE);
    let tableDefs = readPage(fd, pageId, 'createTable');

    let nextOffset = tableDefs.nextOffset;
    let nrTables = tableDefs.recordCount;

    // check if the page has space for a new 64-byte definition
    if (nextOffset + TABLE_SLOT_SIZE > PAGE_SIZE) {
        const newPageId = allocatePage(
            fd,
            PAGE_TYPES.CATALOG_TABLE,
            'createTable',
        );

        // link the current page to the new one
        tableDefs.page.writeUInt32LE(newPageId, NEXT_PAGE_ID_POSITION);
        const written = fs.writeSync(
            fd,
            tableDefs.page,
            0,
            PAGE_SIZE,
            pageId * PAGE_SIZE,
        );
        if (written !== PAGE_SIZE) {
            throw new StorageError(
                StorageErrorCode.SHORT_WRITE,
                'Short Write while linking table definitions page',
                {
                    context: {
                        pageId,
                        expectedBytes: PAGE_SIZE,
                        actualBytes: written,
                        position: pageId * PAGE_SIZE,
                    },
                },
            );
        }

        // create the header for the new definitions page
        tableDefs.page = writeHeader(
            fd,
            newPageId,
            PAGE_TYPES.CATALOG_TABLE,
            'createTable',
        );
        pageId = newPageId;

        nextOffset = 16;
        nrTables = 0;
    }

    let writeOffset = nextOffset;
    // set table name value (zero first to avoid stale bytes from a shorter previous name)
    tableDefs.page.fill(0, writeOffset, writeOffset + 12);
    tableDefs.page.write(name, writeOffset, 'utf8');
    writeOffset += 12;

    // set master null map page id
    const masterNMapPageId = allocatePage(
        fd,
        PAGE_TYPES.MASTER_NULL_MAP,
        'createTable',
    );
    tableDefs.page.writeUInt32LE(masterNMapPageId, writeOffset);
    writeOffset += 4;

    // create the first column definitions page
    const columnDefsId = allocatePage(
        fd,
        PAGE_TYPES.CATALOG_COLUMN,
        'createTable',
    );
    tableDefs.page.writeUInt32LE(columnDefsId, writeOffset);

    // initialise the column definitions page and add all columns
    writeHeader(fd, columnDefsId, PAGE_TYPES.CATALOG_COLUMN, 'createTable');
    columns.forEach((col: Column) => createColumn(fd, columnDefsId, col));

    tableDefs.page.writeUInt16LE(
        nextOffset + TABLE_SLOT_SIZE,
        NEXT_SLOT_OFFSET_POSITION,
    );
    tableDefs.page.writeUInt16LE(nrTables + 1, RECORD_COUNT_POSITION);

    const written = fs.writeSync(
        fd,
        tableDefs.page,
        0,
        PAGE_SIZE,
        pageId * PAGE_SIZE,
    );
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing table definition',
            {
                context: {
                    pageId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written,
                    position: pageId * PAGE_SIZE,
                },
            },
        );
    }
    fs.fsyncSync(fd);
};

const getTable = (name: string, db: DatabaseContext): Table => {
    const cachedTable = db.tableCache.get(name);

    if (cachedTable) {
        return cachedTable;
    }

    const nameBuffer = Buffer.from(name, 'utf8');
    if (nameBuffer.length >= 12) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'The name should not exceed 12 bytes',
        );
    }

    const fd = db.fd;
    let parsedPage = readPage(fd, 1, 'getTable');
    let offset = 16;
    let page = parsedPage.page;
    let nextPageId = parsedPage.nextPageId;
    let tableCount = parsedPage.recordCount;

    while (true) {
        for (let i = 0; i < tableCount; i++) {
            // early break condition as a empty name means no more entries
            if (page[offset] === 0) {
                break;
            }

            // check if the name of the table matches the name we're searching for
            if (
                nameBuffer.compare(page, offset, offset + nameBuffer.length) !==
                    0 ||
                page[offset + nameBuffer.length] !== 0
            ) {
                offset += TABLE_SLOT_SIZE;
                continue;
            }

            offset += 12;

            const masterNMapPageId = page.readUInt32LE(offset);
            offset += 4;

            const colDefsPageId = page.readUInt32LE(offset);
            offset += TABLE_SLOT_SIZE - (12 + 4 + 4);

            // update the cache
            db.tableCache.set(name, {
                name,
                masterNMapPageId,
                colDefsPageId,
            });

            return {
                name,
                masterNMapPageId,
                colDefsPageId,
            };
        }

        // if nextPageId = 0 that means that there are no more table definition pages
        // and that the table with the specified name does not exist
        if (nextPageId === 0) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'There exists no table with the name ' + name,
            );
        }

        // go to the next page
        parsedPage = readPage(fd, nextPageId, 'getTable');
        offset = 16;
        page = parsedPage.page;
        nextPageId = parsedPage.nextPageId;
        tableCount = parsedPage.recordCount;
    }
};

export { createColumn, createTable, isForeignKey, getTable };
