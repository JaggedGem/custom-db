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
    HEADER_SIZE,
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
import { Column, DatabaseContext, isForeignKey, Table } from './types';
import { getColumn, getTable } from './catalog-read';

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

        pageId = newPageId;

        colDefs = readPage(fd, pageId, 'createColumn');

        nextOffset = HEADER_SIZE;
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
        );

        const referencedPageId =
            referencedColumn.type === 'foreign_key' ?
                referencedColumn.foreignKey.refPageId
            :   referencedColumn.columnDataId;

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
            referencedPageId,
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

        nextOffset = HEADER_SIZE;
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

const persistNextRowId = (db: DatabaseContext, table: Table) => {
    const page = readPage(db.fd, table.catalogPageId, 'persistNextRowId');

    page.page.writeUInt32LE(
        table.nextRowId,
        table.catalogSlotOffset + TABLE_SLOT.NEXT_ROW_ID,
    );

    const written = fs.writeSync(
        db.fd,
        page.page,
        0,
        PAGE_SIZE,
        table.catalogPageId * PAGE_SIZE,
    );

    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while updating next row id in the ' +
                table.name +
                ' table',
            {
                context: {
                    pageId: table.catalogPageId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written,
                    position: table.catalogPageId * PAGE_SIZE,
                },
            },
        );
    }

    fs.fsyncSync(db.fd);
};

export { createColumn, createTable, persistNextRowId };
