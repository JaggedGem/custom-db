import fs from 'node:fs';
import { describe, expect, it } from 'vitest';

import { PAGE_SIZE, PAGE_TYPES } from '../src/constants';
import { initDatabase, closeDatabase } from '../src/database';
import {
    allocatePage,
    getLatestPage,
    loadPage,
    readPage,
    verifyPageType,
    writeHeader,
} from '../src/page';
import { StorageError, ValidationError } from '../src/errors';
import { cleanupTempDbFile, createTempDbFile } from './helpers';

describe('page', () => {
    it('writeHeader initializes page header fields', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const pageId = allocatePage(fd, PAGE_TYPES.DATA_FIXED, 'page.test');

            const page = writeHeader(
                fd,
                pageId,
                PAGE_TYPES.DATA_BITMAP,
                'page.test',
            );
            expect(page.readUInt32LE(0)).toBe(0);
            expect(page.readUInt16LE(4)).toBe(16);
            expect(page.readUInt16LE(6)).toBe(0);
            expect(page.readUInt8(15)).toBe(PAGE_TYPES.DATA_BITMAP);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('allocatePage returns next page id and bumps master next free page id', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const pageId = allocatePage(
                fd,
                PAGE_TYPES.DATA_SLOTTED,
                'page.test',
            );

            expect(pageId).toBe(2);
            const header = readPage(fd, 0, 'page.test').page;
            expect(header.readUInt32LE(7)).toBe(3);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('allocatePage rejects unknown page types', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            expect(() => allocatePage(fd, 255 as any, 'page.test')).toThrow(
                ValidationError,
            );
            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('readPage throws on short reads', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            expect(() => readPage(fd, 99, 'page.test')).toThrow(StorageError);
            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('verifyPageType returns true only for matching page type', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const pageId = allocatePage(fd, PAGE_TYPES.DATA_FIXED, 'page.test');

            expect(
                verifyPageType(fd, pageId, 'page.test', PAGE_TYPES.DATA_FIXED),
            ).toBe(true);
            expect(
                verifyPageType(fd, pageId, 'page.test', PAGE_TYPES.DATA_BITMAP),
            ).toBe(false);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('loadPage returns raw page bytes', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const pageId = allocatePage(fd, PAGE_TYPES.DATA_FIXED, 'page.test');
            const page = Buffer.alloc(PAGE_SIZE);
            page.write('HELLO', 0, 'utf8');
            fs.writeSync(fd, page, 0, PAGE_SIZE, pageId * PAGE_SIZE);

            const loaded = loadPage(fd, pageId, 'page.test');
            expect(loaded.toString('utf8', 0, 5)).toBe('HELLO');

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('getLatestPage follows the chain and returns the last page', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const p1 = allocatePage(fd, PAGE_TYPES.CATALOG_COLUMN, 'page.test');
            const p2 = allocatePage(fd, PAGE_TYPES.CATALOG_COLUMN, 'page.test');

            const p1Buf = loadPage(fd, p1, 'page.test');
            p1Buf.writeUInt32LE(p2, 0);
            fs.writeSync(fd, p1Buf, 0, PAGE_SIZE, p1 * PAGE_SIZE);

            expect(getLatestPage(fd, p1, PAGE_TYPES.CATALOG_COLUMN)).toBe(p2);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('getLatestPage throws when chain contains mismatched page type', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const p1 = allocatePage(fd, PAGE_TYPES.CATALOG_COLUMN, 'page.test');
            const p2 = allocatePage(fd, PAGE_TYPES.DATA_FIXED, 'page.test');

            const p1Buf = loadPage(fd, p1, 'page.test');
            p1Buf.writeUInt32LE(p2, 0);
            fs.writeSync(fd, p1Buf, 0, PAGE_SIZE, p1 * PAGE_SIZE);

            expect(() =>
                getLatestPage(fd, p1, PAGE_TYPES.CATALOG_COLUMN),
            ).toThrow(StorageError);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });
});
