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

const DB_FILE = 'data/test.cdb';
const PAGE_SIZE = 4096;

const PAGE_TYPES = {
    CATALOG_TABLE: 1,
    CATALOG_COLUMN: 2,
    DATA_SLOTTED: 3,
    DATA_FIXED: 4,
    DATA_BITMAP: 5
};

const DATA_TYPES = {
    BOOLEAN: 1,
    INTEGER: 2,
    STRING: 3,
    FOREIGN_KEY: 4
};

function initDatabase(filePath: string, overwrite: boolean = false) {
    const fd = fs.openSync(filePath, overwrite ? 'w+' : 'a+');

    const stats = fs.fstatSync(fd);
    if (stats.size === 0 || overwrite) {
        console.log('Empty file detected. Writing header...');

        const header = Buffer.alloc(PAGE_SIZE);

        // writing identificator for file type
        header.write('CDB', 0, 'utf8'); 

        // writing the version (v1)
        header.writeUInt8(1, 4);

        // writing the page size (4096) from offset 
        header.writeUInt16LE(PAGE_SIZE, 5);

        // Next Free Page ID = 3
        // (page 0 is the header & page 1 are the table definitions so the next free page id is 2)
        header.writeUInt32LE(2, 7);

        // save data to disk
        const written = fs.writeSync(fd, header, 0, PAGE_SIZE, 0);
        if (written !== PAGE_SIZE) {
            throw new Error("Short write while writing main file headers");
        }
        
        console.log('header initialized.');

        const tableDefs = Buffer.alloc(PAGE_SIZE);
        tableDefs.writeUInt32LE(0, 0); // default next page id = 0 (no other page exists for now)
        tableDefs.writeUInt16LE(16, 4); // default next offset = 16 (header size = 16 bytes, offset of next offset = 4 bytes)
        tableDefs.writeUInt16LE(0, 6); // default number of tables = 0
        // empty padding (7 bytes)
        tableDefs.writeUInt8(PAGE_TYPES.CATALOG_TABLE, 15); // refer to PAGE_TYPES

        const written1 = fs.writeSync(fd, tableDefs, 0, PAGE_SIZE, PAGE_SIZE);
        if (written1 !== PAGE_SIZE) {
            throw new Error("Short write while writing table definitions headers");
        }

        fs.fsyncSync(fd);
    } else {
        const header = Buffer.alloc(PAGE_SIZE);
        fs.readSync(fd, header, 0, PAGE_SIZE, 0);

        if (header.toString("utf8", 0, 3) !== "CDB") {
            throw new Error("Invalid database file (bad magic)");
        }

        const version = header.readUInt8(4);
        if (version !== 1) {
            throw new Error("Unsupported DB version: " + version);
        }

        const pageSize = header.readUInt16LE(5);
        if (pageSize !== PAGE_SIZE) {
            throw new Error("Page size mismatch");
        }
    }

    return fd;
}

const allocatePage = (fd: number) => {
    const header = Buffer.alloc(PAGE_SIZE);
    fs.readSync(fd, header, 0, PAGE_SIZE, 0);

    const nextId = header.readUInt32LE(7);
    
    const newPage = Buffer.alloc(PAGE_SIZE);
    const written = fs.writeSync(fd, newPage, 0, PAGE_SIZE, nextId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new Error("Short write while allocating page");
    }

    header.writeUInt32LE(nextId + 1, 7);
    const written1 = fs.writeSync(fd, header, 0, PAGE_SIZE, 0);
    if (written1 !== PAGE_SIZE) {
        throw new Error("Short write while writing header to page");
    }

    fs.fsyncSync(fd);

    return nextId;
}

const loadPage = (fd: number, pageId: number) => {
    const diskBuffer = Buffer.alloc(PAGE_SIZE);
    const bytes = fs.readSync(fd, diskBuffer, 0, PAGE_SIZE, pageId * PAGE_SIZE);

    if (bytes !== PAGE_SIZE)
        throw new Error(`Failed to read full page ${pageId}`);

    return diskBuffer;
}

function getLatestPage(fd: number, startingPageId: number): { page: Buffer, pageId: number } {
    let currentPageId = startingPageId;
    let pageBuffer = Buffer.alloc(PAGE_SIZE);

    while (true) {
        pageBuffer = loadPage(fd, currentPageId);
        
        const nextId = pageBuffer.readUInt32LE(0);
        
        // If nextId is 0, this IS the last page in the chain
        if (nextId === 0) {
            return { 
                page: pageBuffer, 
                pageId: currentPageId 
            };
        }
        
        // Otherwise, move to the next page
        currentPageId = nextId;
    }
}

const createBitmapPage = (fd: number) => {
    const pageId = allocatePage(fd);

    const page = loadPage(fd, pageId);
    page.writeUInt32LE(0, 0); // default next page id = 0
    page.writeUInt8(5, 4); // type of page = 5 (bitmap) refer to PAGE_TYPES
    page.writeUInt32LE(0, 8); // total bits
    // padding (data starts at the 16th byte)

    const written = fs.writeSync(fd, page, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new Error("Short write while writing header to bitmap page");
    }
    fs.fsyncSync(fd);

    return pageId;
}

const createFixedPage = (fd: number) => {
    const pageId = allocatePage(fd);

    initChainPage(fd, pageId, PAGE_TYPES.DATA_FIXED);

    return pageId;
}

const createSlottedPage = (fd: number) => {
    const pageId = allocatePage(fd);

    const page = Buffer.alloc(PAGE_SIZE);
    
    // Header Structure
    page.writeUInt32LE(0, 0);       // next page id (0 = end of chain)
    page.writeUInt16LE(16, 4);      // next slot offset (start after 16-byte header)
    page.writeUInt16LE(0, 6);       // record count (0)
    // ... bytes 8-12 are padding ...
    page.writeUInt16LE(PAGE_SIZE, 12); // free space pointer (used for the data stored at the end of the page)
    // ... the 14th byte is reserved ...
    page.writeUInt8(PAGE_TYPES.DATA_SLOTTED, 15);      // page type identifier (refer to PAGE_TYPES)

    // save to disk
    const written = fs.writeSync(fd, page, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new Error("Short write while writing slotted page");
    }
    fs.fsyncSync(fd);

    return pageId;
}

const initChainPage = (fd: number, pageId: number, type: number) => {
    const buf = Buffer.alloc(PAGE_SIZE);
    
    // Header Structure
    buf.writeUInt32LE(0, 0);       // next page id (0 = end of chain)
    buf.writeUInt16LE(16, 4);      // next offset (start after 16-byte header)
    buf.writeUInt16LE(0, 6);       // record count (0)
    // ... bytes 8-14 are padding ...
    buf.writeUInt8(type, 15);      // page type identifier (refer to PAGE_TYPES)

    // save to disk
    const written = fs.writeSync(fd, buf, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new Error("Short write while writing first chain header to page");
    }
    fs.fsyncSync(fd);

    return buf;
}

const isForeignKey = (col: Column): col is ForeignKeyColumn => col.isForeignKey === true;

const createColumn = (fd: number, startingPageId: number, column: Column) => {
    if (Buffer.byteLength(column.name, 'utf8') > 32){
        throw new Error('Column name too long (max 32 bytes)');
    }

    // find the last page (to add new columns)
    const colDefsInfo = getLatestPage(fd, startingPageId);
    let colDefs = colDefsInfo.page;
    let pageId = colDefsInfo.pageId;

    const SLOT_SIZE = 64; // name(32) + type(1) + dataPageId(4) + nextColId(4) + padding
    let nextOffset = colDefs.readUInt16LE(4);
    let nrColumns = colDefs.readUInt16LE(6);

    // check if the page has space for a new 64-byte definition
    if (nextOffset + SLOT_SIZE > PAGE_SIZE) {
        const newPageId = allocatePage(fd);
        
        // link the current page to the new one
        colDefs.writeUInt32LE(newPageId, 0);
        const written = fs.writeSync(fd, colDefs, 0, PAGE_SIZE, pageId * PAGE_SIZE);
        if (written !== PAGE_SIZE) {
            throw new Error("Short write while editing main column definitions page");
        }

        // create the header for the new definitions page
        colDefs = initChainPage(fd, newPageId, PAGE_TYPES.CATALOG_COLUMN);
        pageId = newPageId;

        nextOffset = 16;
        nrColumns = 0;
    }

    // set column name (32 bytes)
    colDefs.fill(0, nextOffset, nextOffset + 32); // clean before write
    colDefs.write(column.name, nextOffset, 'utf8');
    
    // set the type of data (at offset 32 from current start)
    const typeOffset = nextOffset + 32;
    let columnDataId = 0;

    if (!isForeignKey(column)) {
        if (column.type === 'boolean') {
            columnDataId = createBitmapPage(fd);
            colDefs.writeUInt8(DATA_TYPES.BOOLEAN, typeOffset);
        } else if (column.type === 'integer') {
            columnDataId = createFixedPage(fd);
            colDefs.writeUInt8(DATA_TYPES.INTEGER, typeOffset);
        } else if (column.type === 'string') {
            columnDataId = createSlottedPage(fd);
            colDefs.writeUInt8(DATA_TYPES.STRING, typeOffset);
        }
    } else {
        colDefs.writeUInt8(DATA_TYPES.FOREIGN_KEY, typeOffset);

        
        columnDataId = createFixedPage(fd);
        
        if (Buffer.byteLength(column.foreignKey.table, "utf8") > 12) {
            throw new Error("Foreign key table name too long (max 12 bytes)");
        }
        if (Buffer.byteLength(column.foreignKey.column, "utf8") > 12) {
            throw new Error("Foreign key column name too long (max 12 bytes)");
        }

        // target table (offset 37)
        colDefs.fill(0, typeOffset + 5, typeOffset + 17);
        colDefs.write(column.foreignKey.table, typeOffset + 5, 'utf8');

        // target column (offset 49{37 + 12})
        colDefs.fill(0, typeOffset + 17, typeOffset + 29);
        colDefs.write(column.foreignKey.column, typeOffset + 17, 'utf8');
    }

    // set the starting page for the actual data (at offset 33)
    colDefs.writeUInt32LE(columnDataId, nextOffset + 33);

    // update metadata for this page
    colDefs.writeUInt16LE(nextOffset + SLOT_SIZE, 4); // update next offset
    colDefs.writeUInt16LE(nrColumns + 1, 6);        // increment column count

    // save to disk
    const written = fs.writeSync(fd, colDefs, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new Error("Short write while writing column to columns page");
    }
    fs.fsyncSync(fd);

    return columnDataId;
}

const createTable = (fd: number, name: string, columns: Column[]) => {
    if (Buffer.byteLength(name, 'utf8') > 12)
        throw new Error('Table name too long (max 12 bytes)');
    
    const SLOT_SIZE = 64;

    const tableDefsPage = getLatestPage(fd, 1);
    let tableDefs = tableDefsPage.page;
    let pageId = tableDefsPage.pageId;

    // read metadata from header
    let nextOffset = tableDefs.readUInt16LE(4);
    let nrTables = tableDefs.readUInt16LE(6);

    // check if the page has space for a new 64-byte definition
    if (nextOffset + SLOT_SIZE > PAGE_SIZE) {
        const newPageId = allocatePage(fd);
        
        // link the current page to the new one
        tableDefs.writeUInt32LE(newPageId, 0);
        const written = fs.writeSync(fd, tableDefs, 0, PAGE_SIZE, pageId * PAGE_SIZE);
        if (written !== PAGE_SIZE) {
            throw new Error("Short write while allocating page");
        }

        // create the header for the new definitions page
        tableDefs = initChainPage(fd, newPageId, PAGE_TYPES.CATALOG_TABLE);
        pageId = newPageId;

        nextOffset = 16;
        nrTables = 0;
    }

    let writeOffset = nextOffset;
    // set table name value
    tableDefs.write(name, writeOffset, 'utf8');
    writeOffset += 12;

    // set master null map page id
    const masterNMapPageId = allocatePage(fd);
    tableDefs.writeUInt32LE(masterNMapPageId, writeOffset);
    writeOffset += 4;

    // create the first column definitions page
    const columnDefsId = allocatePage(fd);
    tableDefs.writeUInt32LE(columnDefsId, writeOffset);

    // add metadata
    initChainPage(fd, columnDefsId, PAGE_TYPES.CATALOG_COLUMN);
    
    // add all columns to the column definitions
    columns.forEach((col: Column) => createColumn(fd, columnDefsId, col));

    tableDefs.writeUInt16LE(nextOffset + SLOT_SIZE, 4);
    tableDefs.writeUInt16LE(nrTables + 1, 6);

    const written = fs.writeSync(fd, tableDefs, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new Error("Short write while writing table definitions page");
    }
    fs.fsyncSync(fd);
}


export {
  initDatabase,
  allocatePage,
  loadPage,
  getLatestPage,
  createBitmapPage,
  createFixedPage,
  createSlottedPage,
  initChainPage,
  createColumn,
  createTable,
  PAGE_SIZE
};