import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import path from 'path';

import {
    initDatabase,
    allocatePage,
    loadPage,
    getLatestPage,
    createBitmapPage,
    createFixedPage,
    createSlottedPage,
    writeHeader,
    createColumn,
    createTable,
    PAGE_SIZE,
    PAGE_TYPES,
    DATA_TYPES,
    closeDatabase,
} from '../src/index';

const TEST_DB = path.join(__dirname, 'test.cdb');

beforeEach(() => {
    if (fs.existsSync(TEST_DB)) fs.unlinkSync(TEST_DB);
});

afterEach(() => {
    if (fs.existsSync(TEST_DB)) fs.unlinkSync(TEST_DB);
});

describe('initDatabase', () => {
    it('writes correct header', () => {
        const fd = initDatabase(TEST_DB, true);
        const page0 = loadPage(fd, 0, 'initDatabase.test');

        expect(page0.toString('utf8', 0, 3)).toBe('CDB');
        expect(page0.readUInt8(4)).toBe(1);
        expect(page0.readUInt16LE(5)).toBe(PAGE_SIZE);
        expect(page0.readUInt32LE(7)).toBe(2);

        closeDatabase(fd);
    });

    it('creates catalog table page', () => {
        const fd = initDatabase(TEST_DB, true);
        const page1 = loadPage(fd, 1, 'initDatabase.test');

        expect(page1.readUInt32LE(0)).toBe(0); // next page id = 0
        expect(page1.readUInt16LE(4)).toBe(16); // next slot offset starts after 16-byte header
        expect(page1.readUInt16LE(6)).toBe(0); // 0 tables
        expect(page1.readUInt8(15)).toBe(PAGE_TYPES.CATALOG_TABLE); // page type

        closeDatabase(fd);
    });
});

describe('allocatePage', () => {
    it('allocates new page and increments header', () => {
        const fd = initDatabase(TEST_DB, true);

        const pageId = allocatePage(
            fd,
            PAGE_TYPES.DATA_SLOTTED,
            'allocatePage.test',
        );
        expect(pageId).toBe(2);

        const header = loadPage(fd, 0, 'allocatePage.test');
        expect(header.readUInt32LE(7)).toBe(3);

        closeDatabase(fd);
    });
});

describe('loadPage', () => {
    it('reads written data', () => {
        const fd = initDatabase(TEST_DB, true);
        const buf = Buffer.alloc(PAGE_SIZE);
        buf.write('HELLO');

        fs.writeSync(fd, buf, 0, PAGE_SIZE, PAGE_SIZE * 2);

        const loaded = loadPage(fd, 2, 'loadPage.test');
        expect(loaded.toString('utf8', 0, 5)).toBe('HELLO');

        closeDatabase(fd);
    });
});

describe('getLatestPage', () => {
    it('returns last page in chain', () => {
        const fd = initDatabase(TEST_DB, true);

        const p1 = allocatePage(
            fd,
            PAGE_TYPES.CATALOG_COLUMN,
            'getLatestPage.test',
        );
        const p2 = allocatePage(
            fd,
            PAGE_TYPES.CATALOG_COLUMN,
            'getLatestPage.test',
        );

        const page = loadPage(fd, p1, 'getLatestPage.test');
        page.writeUInt32LE(p2, 0);
        fs.writeSync(fd, page, 0, PAGE_SIZE, p1 * PAGE_SIZE);

        const res = getLatestPage(fd, p1, PAGE_TYPES.CATALOG_COLUMN);
        expect(res).toBe(p2);

        closeDatabase(fd);
    });
});

describe('createBitmapPage', () => {
    it('creates bitmap page', () => {
        const fd = initDatabase(TEST_DB, true);
        const pageId = createBitmapPage(fd);
        const page = loadPage(fd, pageId, 'createBitmapPage.test');

        expect(page.readUInt8(15)).toBe(PAGE_TYPES.DATA_BITMAP);
        expect(page.readUInt32LE(8)).toBe(0); // total bits initialised to 0

        closeDatabase(fd);
    });
});

describe('createFixedPage', () => {
    it('creates fixed page', () => {
        const fd = initDatabase(TEST_DB, true);
        const pageId = createFixedPage(fd);
        const page = loadPage(fd, pageId, 'createFixedPage.test');

        expect(page.readUInt8(15)).toBe(PAGE_TYPES.DATA_FIXED);

        closeDatabase(fd);
    });
});

describe('createSlottedPage', () => {
    it('creates slotted page', () => {
        const fd = initDatabase(TEST_DB, true);
        const pageId = createSlottedPage(fd);
        const page = loadPage(fd, pageId, 'createSlottedPage.test');

        expect(page.readUInt16LE(4)).toBe(16); // next slot offset starts after header
        expect(page.readUInt16LE(6)).toBe(0); // 0 records
        expect(page.readUInt8(15)).toBe(PAGE_TYPES.DATA_SLOTTED); // page type

        closeDatabase(fd);
    });
});

describe('writeHeader', () => {
    it('initializes page header correctly and returns buffer', () => {
        const fd = initDatabase(TEST_DB, true);
        const pid = allocatePage(
            fd,
            PAGE_TYPES.CATALOG_TABLE,
            'writeHeader.test',
        );

        const page = writeHeader(fd, pid, PAGE_TYPES.CATALOG_TABLE, 'writeHeader.test');
        expect(page.readUInt32LE(0)).toBe(0);
        expect(page.readUInt8(15)).toBe(PAGE_TYPES.CATALOG_TABLE);

        closeDatabase(fd);
    });
});

describe('createColumn', () => {
    it('creates normal column', () => {
        const fd = initDatabase(TEST_DB, true);

        const colPage = allocatePage(
            fd,
            PAGE_TYPES.CATALOG_COLUMN,
            'createColumn.test',
        );
        writeHeader(fd, colPage, PAGE_TYPES.CATALOG_COLUMN, 'createColumn.test');

        const dataPage = createColumn(fd, colPage, {
            name: 'age',
            type: 'integer',
            isForeignKey: false,
        });

        expect(typeof dataPage).toBe('number');

        closeDatabase(fd);
    });

    it('creates foreign key column', () => {
        const fd = initDatabase(TEST_DB, true);

        const colPage = allocatePage(
            fd,
            PAGE_TYPES.CATALOG_COLUMN,
            'createColumn.test',
        );
        writeHeader(fd, colPage, PAGE_TYPES.CATALOG_COLUMN, 'createColumn.test');

        createColumn(fd, colPage, {
            name: 'user_id',
            isForeignKey: true,
            foreignKey: {
                table: 'users',
                column: 'id',
            },
        });

        const page = loadPage(fd, colPage, 'createColumn.test');
        // slot starts at offset 16; name is 12 bytes, so type byte is at 16 + 12 = 28
        const type = page.readUInt8(16 + 12);
        expect(type).toBe(DATA_TYPES.FOREIGN_KEY);

        closeDatabase(fd);
    });
});

describe('createTable', () => {
    it('creates table with columns', () => {
        const fd = initDatabase(TEST_DB, true);

        createTable(fd, 'users', [
            { name: 'id', type: 'integer', isForeignKey: false },
            { name: 'name', type: 'string', isForeignKey: false },
        ]);

        const page1 = loadPage(fd, 1, 'createTable.test');
        expect(page1.readUInt16LE(6)).toBe(1); // 1 table registered

        closeDatabase(fd);
    });

    it('rejects long table names', () => {
        const fd = initDatabase(TEST_DB, true);

        expect(() =>
            createTable(fd, 'this_name_is_way_too_long', []),
        ).toThrow();

        closeDatabase(fd);
    });
});
