import * as fs from 'fs';

interface BaseColumn {
    name: string;
}

interface ForeignKeyColumn extends BaseColumn {
    isForeignKey: true;
    foreignKey: {
        table: string;
        column: string;
    };
}

interface NormalColumn extends BaseColumn {
    type: 'boolean' | 'integer' | 'string';
    isForeignKey: false;
    foreignKey?: never;
}

type Column = ForeignKeyColumn | NormalColumn;

const PAGE_SIZE = 4096;

const NEXT_PAGE_ID_POSITION = 0;
const NEXT_SLOT_OFFSET_POSITION = 4;
const RECORD_COUNT_POSITION = 6;
const PAGE_TYPE_POSITION = 15;

const PAGE_TYPES = {
    MASTER_NULL_MAP: 1,
    CATALOG_TABLE: 2,
    CATALOG_COLUMN: 3,
    DATA_SLOTTED: 4,
    DATA_FIXED: 5,
    DATA_BITMAP: 6,
} as const;

const DATA_TYPES = {
    BOOLEAN: 1,
    INTEGER: 2,
    STRING: 3,
    FOREIGN_KEY: 4,
};

const enum StorageErrorCode {
    SHORT_WRITE = 'SHORT_WRITE',
    SHORT_READ = 'SHORT_READ',
    IO_ERROR = 'IO_ERROR',
}

export class StorageError extends Error {
    readonly code: StorageErrorCode;
    readonly context?: Record<string, unknown>;
    readonly cause?: unknown;

    constructor(
        code: StorageErrorCode,
        message: string,
        options?: {
            context?: Record<string, unknown>;
            cause?: unknown;
        },
    ) {
        super(message);
        this.name = 'StorageError';
        this.code = code;

        if (options?.context !== undefined) {
            this.context = options.context;
        }

        if (options?.cause !== undefined) {
            this.cause = options.cause;
        }
    }
}

const enum ValidationErrorCode {
    BAD_INPUT = 'BAD_INPUT',
}
export class ValidationError extends Error {
    readonly code: ValidationErrorCode;
    readonly context?: Record<string, unknown>;
    readonly cause?: unknown;

    constructor(
        code: ValidationErrorCode,
        message: string,
        options?: {
            context?: Record<string, unknown>;
            cause?: unknown;
        },
    ) {
        super(message);
        this.name = 'ValidationError';
        this.code = code;

        if (options?.context !== undefined) {
            this.context = options.context;
        }

        if (options?.cause !== undefined) {
            this.cause = options.cause;
        }
    }
}

type PageType = (typeof PAGE_TYPES)[keyof typeof PAGE_TYPES];

const writeHeader = (
    fd: number,
    pageId: number,
    pageType: PageType,
    caller: string,
) => {
    const buf = Buffer.alloc(PAGE_SIZE);

    // Header Structure
    buf.writeUInt32LE(0, NEXT_PAGE_ID_POSITION); // next page id (0 = end of chain)
    buf.writeUInt16LE(16, NEXT_SLOT_OFFSET_POSITION); // next offset (start after 16-byte header)
    buf.writeUInt16LE(0, RECORD_COUNT_POSITION); // record count (0)
    // ... bytes 8-14 are padding ...
    buf.writeUInt8(pageType, PAGE_TYPE_POSITION); // page type identifier (refer to PAGE_TYPES)

    const written = fs.writeSync(fd, buf, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing page header',
            {
                context: {
                    caller,
                    pageId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written,
                    position: pageId * PAGE_SIZE,
                },
            },
        );
    }
};

const readPage = (fd: number, pageId: number, caller: string) => {
    const buf = Buffer.alloc(PAGE_SIZE);
    const bytes = fs.readSync(fd, buf, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (bytes !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_READ,
            'Short Read while reading page',
            {
                context: {
                    caller,
                    pageId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: bytes,
                    position: pageId * PAGE_SIZE,
                },
            },
        );
    }

    return {
        nextPageId: buf.readUInt32LE(NEXT_PAGE_ID_POSITION),
        nextOffset: buf.readUInt16LE(NEXT_SLOT_OFFSET_POSITION),
        recordCount: buf.readUInt16LE(RECORD_COUNT_POSITION),
        pageType: buf.readUInt8(PAGE_TYPE_POSITION),
        page: buf,
    };
};

const verifyPageType = (
    fd: number,
    pageId: number,
    caller: string,
    expectedType: PageType,
) => {
    const { pageType } = readPage(fd, pageId, 'verifyPageType/' + caller);
    return expectedType === pageType;
};

function initDatabase(filePath: string, overwrite: boolean = false) {
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
}

const closeDatabase = (fd: number) => {
    fs.fsyncSync(fd);
    fs.closeSync(fd);
};

const allocatePage = (fd: number, pageType: PageType, caller: string) => {
    if (!Object.values(PAGE_TYPES).includes(pageType)) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'Invalid Page Type',
            {
                context: {
                    caller,
                    expectedValue: Object.values(PAGE_TYPES),
                    actualValue: pageType,
                },
            },
        );
    }

    // Use readPage so the master header read is checked for short reads
    const { page: header } = readPage(fd, 0, 'allocatePage/' + caller);

    const nextId = header.readUInt32LE(7);

    const newPage = Buffer.alloc(PAGE_SIZE);
    const written = fs.writeSync(fd, newPage, 0, PAGE_SIZE, nextId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while allocating new page',
            {
                context: {
                    caller,
                    pageId: nextId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written,
                    position: nextId * PAGE_SIZE,
                },
            },
        );
    }

    writeHeader(fd, nextId, pageType, 'allocatePage/' + caller);

    header.writeUInt32LE(nextId + 1, 7);
    const written1 = fs.writeSync(fd, header, 0, PAGE_SIZE, 0);
    if (written1 !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing new next page id',
            {
                context: {
                    caller,
                    pageId: 0,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written1,
                    position: 0,
                },
            },
        );
    }

    fs.fsyncSync(fd);

    return nextId;
};

const loadPage = (fd: number, pageId: number, caller: string) => {
    return readPage(fd, pageId, caller).page;
};

function getLatestPage(
    fd: number,
    startingPageId: number,
    expectedType: PageType,
) {
    let currentPageId = startingPageId;

    while (true) {
        // Single read per iteration — use pageType directly instead of a
        // second verifyPageType call which would re-read the same page.
        const page = readPage(fd, currentPageId, 'getLatestPage');

        if (page.pageType !== expectedType) {
            throw new StorageError(
                StorageErrorCode.IO_ERROR,
                'Pages in chain should have the same page types',
                {
                    context: {
                        pageId: currentPageId,
                        expectedType,
                        actualType: page.pageType,
                    },
                },
            );
        }

        // If nextPageId is 0, this IS the last page in the chain
        if (page.nextPageId === 0) {
            return currentPageId;
        }

        // Otherwise, move to the next page
        currentPageId = page.nextPageId;
    }
}

const createBitmapPage = (fd: number) => {
    const pageId = allocatePage(fd, PAGE_TYPES.DATA_BITMAP, 'createBitmapPage');

    const { page } = readPage(fd, pageId, 'createBitmapPage');
    page.writeUInt32LE(0, 8); // total bits (bitmap-specific field)

    const written = fs.writeSync(fd, page, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing bitmap page',
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

    return pageId;
};

const createFixedPage = (fd: number) => {
    const pageId = allocatePage(fd, PAGE_TYPES.DATA_FIXED, 'createFixedPage');

    initChainPage(fd, pageId, PAGE_TYPES.DATA_FIXED);

    return pageId;
};

const createSlottedPage = (fd: number) => {
    const pageId = allocatePage(
        fd,
        PAGE_TYPES.DATA_SLOTTED,
        'createSlottedPage',
    );

    const page = Buffer.alloc(PAGE_SIZE);

    // Header Structure
    page.writeUInt32LE(0, NEXT_PAGE_ID_POSITION); // next page id (0 = end of chain)
    page.writeUInt16LE(16, NEXT_SLOT_OFFSET_POSITION); // next slot offset (start after 16-byte header)
    page.writeUInt16LE(0, RECORD_COUNT_POSITION); // record count (0)
    // ... bytes 8-12 are padding ...
    page.writeUInt16LE(PAGE_SIZE, 12); // free space pointer (data grows from end of page)
    // ... byte 14 is reserved ...
    page.writeUInt8(PAGE_TYPES.DATA_SLOTTED, PAGE_TYPE_POSITION); // page type identifier (refer to PAGE_TYPES)

    const written = fs.writeSync(fd, page, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing slotted page',
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

    return pageId;
};

const initChainPage = (fd: number, pageId: number, type: number) => {
    const buf = Buffer.alloc(PAGE_SIZE);

    // Header Structure
    buf.writeUInt32LE(0, NEXT_PAGE_ID_POSITION); // next page id (0 = end of chain)
    buf.writeUInt16LE(16, NEXT_SLOT_OFFSET_POSITION); // next offset (start after 16-byte header)
    buf.writeUInt16LE(0, RECORD_COUNT_POSITION); // record count (0)
    // ... bytes 8-14 are padding ...
    buf.writeUInt8(type, PAGE_TYPE_POSITION); // page type identifier (refer to PAGE_TYPES)

    const written = fs.writeSync(fd, buf, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing chain page header',
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

    return buf;
};

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

    // name(12) + type(1) + dataPageId(4) + fkTableName(12) + fkColumnName(12) + padding(7)
    const SLOT_SIZE = 48;
    let nextOffset = colDefs.nextOffset;
    let nrColumns = colDefs.recordCount;

    // check if the page has space for a new 48-byte definition
    if (nextOffset + SLOT_SIZE > PAGE_SIZE) {
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
        colDefs.page = initChainPage(fd, newPageId, PAGE_TYPES.CATALOG_COLUMN);
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
        nextOffset + SLOT_SIZE,
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

    const SLOT_SIZE = 64;

    let pageId = getLatestPage(fd, 1, PAGE_TYPES.CATALOG_TABLE);
    let tableDefs = readPage(fd, pageId, 'createTable');

    let nextOffset = tableDefs.nextOffset;
    let nrTables = tableDefs.recordCount;

    // check if the page has space for a new 64-byte definition
    if (nextOffset + SLOT_SIZE > PAGE_SIZE) {
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
        tableDefs.page = initChainPage(fd, newPageId, PAGE_TYPES.CATALOG_TABLE);
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
    initChainPage(fd, columnDefsId, PAGE_TYPES.CATALOG_COLUMN);
    columns.forEach((col: Column) => createColumn(fd, columnDefsId, col));

    tableDefs.page.writeUInt16LE(
        nextOffset + SLOT_SIZE,
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

export {
    // core API
    initDatabase,
    closeDatabase,
    allocatePage,
    loadPage,
    getLatestPage,
    createBitmapPage,
    createFixedPage,
    createSlottedPage,
    initChainPage,
    createColumn,
    createTable,

    // util / helper functions
    writeHeader,
    readPage,
    verifyPageType,
    isForeignKey,

    // constants
    PAGE_SIZE,
    PAGE_TYPES,
    DATA_TYPES,
};
