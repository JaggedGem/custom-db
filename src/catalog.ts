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
    COL_SLOT,
    TABLE_SLOT,
    DATA_TYPE_LOOKUP,
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
import {
    Column,
    ForeignKeyColumn,
    DatabaseContext,
    Table,
    ResolvedColumn,
    ResolvedFKColumn,
} from './types';

const isForeignKey = (col: Column): col is ForeignKeyColumn =>
    col.type === 'foreign_key';

// todo: implement existing column checking
const createColumn = (
    db: DatabaseContext,
    startingPageId: number,
    column: Column,
) => {
    if (Buffer.byteLength(column.name, 'utf8') >= 12) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'Column name should be smaller than 12 bytes',
            {
                context: {
                    expectedValue: '<=12',
                    actualValue: Buffer.byteLength(column.name, 'utf8'),
                },
            },
        );
    }

    const fd = db.fd;

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
    colDefs.page.fill(
        0,
        nextOffset + COL_SLOT.NAME,
        nextOffset + COL_SLOT.TYPE,
    ); // clean before write

    colDefs.page.write(column.name, nextOffset + COL_SLOT.NAME, 'utf8');

    // set the type of data (at offset 12 from slot start)
    let columnDataId = 0;

    if (!isForeignKey(column)) {
        if (column.type === 'boolean') {
            columnDataId = createBitmapPage(fd);
            colDefs.page.writeUInt8(
                DATA_TYPES.BOOLEAN,
                nextOffset + COL_SLOT.TYPE,
            );
        } else if (column.type === 'integer') {
            columnDataId = createFixedPage(fd);
            colDefs.page.writeUInt8(
                DATA_TYPES.INTEGER,
                nextOffset + COL_SLOT.TYPE,
            );
        } else if (column.type === 'string') {
            columnDataId = createSlottedPage(fd);
            colDefs.page.writeUInt8(
                DATA_TYPES.STRING,
                nextOffset + COL_SLOT.TYPE,
            );
        }
    } else if (column.type === 'foreign_key') {
        if (!column.foreignKey.column || !column.foreignKey.table) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'Cannot create foreign key column without a target table or column',
                {
                    context: {
                        expectedValue: 'not null',
                        actualValue: 'null',
                        offendingField:
                            (
                                !column.foreignKey.column &&
                                !column.foreignKey.table
                            ) ?
                                'fkTargetColumn fkTargetTable'
                            : !column.foreignKey.column ? 'fkTargetColumn'
                            : 'fkTargetTable',
                    },
                },
            );
        }
        colDefs.page.writeUInt8(
            DATA_TYPES.FOREIGN_KEY,
            nextOffset + COL_SLOT.TYPE,
        );

        // get and write (to the page) the referenced table
        const referencedTable = getTable(column.foreignKey.table, db);

        colDefs.page.fill(
            0,
            nextOffset + COL_SLOT.FK_TABLE,
            nextOffset + COL_SLOT.FK_COL,
        ); // clean before write

        colDefs.page.write(
            referencedTable.name,
            nextOffset + COL_SLOT.FK_TABLE,
            'utf8',
        );

        // get and write (to the page) the referenced column
        const referencedColumn = getColumn(
            column.foreignKey.column,
            referencedTable,
            db,
        ) as ResolvedFKColumn;

        colDefs.page.fill(
            0,
            nextOffset + COL_SLOT.FK_COL,
            nextOffset + COL_SLOT.FK_REF_PAGE_ID,
        ); // clean before write

        colDefs.page.write(
            referencedColumn.name,
            nextOffset + COL_SLOT.FK_COL,
            'utf8',
        );

        // target primary key data page (offset 37 from slot start)
        colDefs.page.writeUInt32LE(
            referencedColumn.foreignKey.refPageId,
            nextOffset + COL_SLOT.FK_REF_PAGE_ID,
        );

        // create the initial page which will contain the actual data
        columnDataId = createFixedPage(fd);
    }

    // write the initial page id (at offset 13 from slot start)
    colDefs.page.writeUInt32LE(
        columnDataId,
        nextOffset +
            (column.type === 'foreign_key' ?
                COL_SLOT.FK_DATA_PAGE_ID
            :   COL_SLOT.DATA_PAGE_ID),
    );

    // update metadata for this page
    colDefs.page.writeUInt16LE(
        nextOffset + COLUMN_SLOT_SIZE,
        NEXT_SLOT_OFFSET_POSITION,
    );

    colDefs.page.writeUInt16LE(nrColumns + 1, RECORD_COUNT_POSITION);

    // write the page to disk
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

// todo: implement existing table checking
const createTable = (db: DatabaseContext, name: string, columns: Column[]) => {
    if (Buffer.byteLength(name, 'utf8') >= 12) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'Table name should be smaller than 12 bytes',
            {
                context: {
                    expectedValue: '<=12',
                    actualValue: Buffer.byteLength(name, 'utf8'),
                },
            },
        );
    }

    const fd = db.fd;

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

        pageId = newPageId;

        nextOffset = 16;
        nrTables = 0;
    }

    // set table name value (zero first to avoid stale bytes from a shorter previous name)
    tableDefs.page.fill(
        0,
        nextOffset + TABLE_SLOT.NAME,
        nextOffset + TABLE_SLOT.MASTER_NMAP_PAGE_ID,
    );

    tableDefs.page.write(name, nextOffset + TABLE_SLOT.NAME, 'utf8');

    // set master null map page id
    const masterNMapPageId = allocatePage(
        fd,
        PAGE_TYPES.MASTER_NULL_MAP,
        'createTable',
    );

    tableDefs.page.writeUInt32LE(
        masterNMapPageId,
        nextOffset + TABLE_SLOT.MASTER_NMAP_PAGE_ID,
    );

    // add nextRowId = 0 to indicate where the data should go
    // (by default 0 meaning the first)
    tableDefs.page.writeUInt32LE(0, nextOffset + TABLE_SLOT.NEXT_ROW_ID);

    // create the first slot map page
    const slotMapId = allocatePage(fd, PAGE_TYPES.SLOT_MAP, 'createTable');

    tableDefs.page.writeUInt32LE(slotMapId, nextOffset + TABLE_SLOT.SLOT_MAP);

    // create the first column definitions page
    const columnDefsId = allocatePage(
        fd,
        PAGE_TYPES.CATALOG_COLUMN,
        'createTable',
    );

    tableDefs.page.writeUInt32LE(
        columnDefsId,
        nextOffset + TABLE_SLOT.COL_DEFS,
    );

    // initialise the column definitions page and add all columns
    columns.forEach((col: Column) => createColumn(db, columnDefsId, col));

    // update the next slot offset
    tableDefs.page.writeUInt16LE(
        nextOffset + TABLE_SLOT_SIZE,
        NEXT_SLOT_OFFSET_POSITION,
    );

    // update the table count
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
            if (page[offset + TABLE_SLOT.NAME] === 0) {
                break;
            }

            // check if the name of the table matches the name we're searching for
            if (
                nameBuffer.compare(
                    page,
                    offset + TABLE_SLOT.NAME,
                    offset + TABLE_SLOT.NAME + nameBuffer.length,
                ) !== 0 ||
                page[offset + TABLE_SLOT.NAME + nameBuffer.length] !== 0
            ) {
                offset += TABLE_SLOT_SIZE;
                continue;
            }

            const masterNMapPageId = page.readUInt32LE(
                offset + TABLE_SLOT.MASTER_NMAP_PAGE_ID,
            );

            const nextRowId = page.readUInt32LE(
                offset + TABLE_SLOT.NEXT_ROW_ID,
            );

            const slotMapId = page.readUInt32LE(offset + TABLE_SLOT.SLOT_MAP);

            const colDefsPageId = page.readUInt32LE(
                offset + TABLE_SLOT.COL_DEFS,
            );

            // update the cache
            db.tableCache.set(name, {
                name,
                masterNMapPageId,
                nextRowId,
                slotMapId,
                colDefsPageId,
            });

            return {
                name,
                masterNMapPageId,
                nextRowId,
                slotMapId,
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

const getColumn = (
    name: string,
    table: Table,
    db: DatabaseContext,
): ResolvedColumn => {
    const cachedColumn = db.columnCache.get(table.name)?.get(name);

    if (cachedColumn) {
        return cachedColumn;
    }

    const nameBuffer = Buffer.from(name, 'utf8');

    if (nameBuffer.length >= 12) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'The name should not exceed 12 bytes',
        );
    }

    const fd = db.fd;
    let parsedPage = readPage(fd, table.colDefsPageId, 'getColumn');
    let offset = 16;
    let page = parsedPage.page;
    let nextPageId = parsedPage.nextPageId;
    let columnCount = parsedPage.recordCount;

    while (true) {
        for (let i = 0; i < columnCount; i++) {
            // early break condition as a empty name means no more entries
            if (page[offset + COL_SLOT.NAME] === 0) {
                break;
            }

            // check if the name of the table matches the name we're searching for
            if (
                nameBuffer.compare(
                    page,
                    offset + COL_SLOT.NAME,
                    offset + COL_SLOT.NAME + nameBuffer.length,
                ) !== 0 ||
                page[offset + COL_SLOT.NAME + nameBuffer.length] !== 0
            ) {
                offset += COLUMN_SLOT_SIZE;
                continue;
            }

            const dataType = page.readUInt8(offset + COL_SLOT.TYPE);

            let fkTableName = '',
                fkColumnName = '',
                fkRefPageId = 0;
            if (dataType === DATA_TYPES.FOREIGN_KEY) {
                let endOffset, nullPos;

                // find the exact foreign key reference names (remove any trailing null bytes)
                nullPos = page.indexOf(0, offset + COL_SLOT.FK_TABLE);
                endOffset =
                    nullPos === -1 || nullPos > offset + COL_SLOT.FK_COL ?
                        offset + COL_SLOT.FK_COL
                    :   nullPos;

                fkTableName = page.toString(
                    'utf8',
                    offset + COL_SLOT.FK_TABLE,
                    endOffset,
                );

                nullPos = page.indexOf(0, offset + COL_SLOT.FK_COL);

                endOffset =
                    (
                        nullPos === -1 ||
                        nullPos > offset + COL_SLOT.FK_REF_PAGE_ID
                    ) ?
                        offset + COL_SLOT.FK_REF_PAGE_ID
                    :   nullPos;

                fkColumnName = page.toString(
                    'utf8',
                    offset + COL_SLOT.FK_COL,
                    endOffset,
                );

                fkRefPageId = page.readUInt32LE(
                    offset + COL_SLOT.FK_REF_PAGE_ID,
                );
            }

            const columnDataId = page.readUInt32LE(
                offset +
                    (dataType === DATA_TYPES.FOREIGN_KEY ?
                        COL_SLOT.FK_DATA_PAGE_ID
                    :   COL_SLOT.DATA_PAGE_ID),
            );

            const type = DATA_TYPE_LOOKUP[
                dataType as keyof typeof DATA_TYPE_LOOKUP
            ] as Column['type'];

            if (!type) {
                throw new ValidationError(
                    ValidationErrorCode.BAD_INPUT,
                    'Bad data type',
                );
            }

            // update the cache (tableTBU - table to be updated)
            if (type === 'foreign_key') {
                let tableTBU = db.columnCache.get(table.name);

                if (!tableTBU) {
                    tableTBU = new Map<string, ResolvedColumn>();
                    db.columnCache.set(table.name, tableTBU);
                }

                tableTBU.set(name, {
                    name,
                    type,
                    columnDataId,
                    foreignKey: {
                        table: fkTableName,
                        column: fkColumnName,
                        refPageId: fkRefPageId,
                    },
                });

                return {
                    name,
                    type,
                    columnDataId,
                    foreignKey: {
                        table: fkTableName,
                        column: fkColumnName,
                        refPageId: fkRefPageId,
                    },
                };
            } else {
                let tableTBU = db.columnCache.get(table.name);

                if (!tableTBU) {
                    tableTBU = new Map<string, ResolvedColumn>();
                    db.columnCache.set(table.name, tableTBU);
                }

                tableTBU.set(name, {
                    name,
                    type,
                    columnDataId,
                });

                return {
                    name,
                    type,
                    columnDataId,
                };
            }
        }

        // if nextPageId = 0 that means that there are no more column definition pages
        // and that the column with the specified name does not exist
        if (nextPageId === 0) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'There exists no column with the name ' + name,
            );
        }

        // go to the next page
        parsedPage = readPage(fd, nextPageId, 'getColumn');
        offset = 16;
        page = parsedPage.page;
        nextPageId = parsedPage.nextPageId;
        columnCount = parsedPage.recordCount;
    }
};

export { createColumn, createTable, isForeignKey, getTable, getColumn };
