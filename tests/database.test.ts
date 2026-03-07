import fs from 'node:fs';
import { describe, expect, it } from 'vitest';

import {
    MH_IDENTIFICATOR_POSITION,
    MH_PAGE_SIZE_POSITION,
    MH_VERSION_POSITION,
    PAGE_SIZE,
    PAGE_TYPES,
} from '../src/constants';
import { initDatabase, closeDatabase } from '../src/database';
import { readPage } from '../src/page';
import { StorageError } from '../src/errors';
import { cleanupTempDbFile, createTempDbFile } from './helpers';

describe('database', () => {
    it('creates a fresh DB with valid header and catalog table page', () => {
        const dbPath = createTempDbFile();

        try {
            const fd = initDatabase(dbPath, true);
            const header = readPage(fd, 0, 'database.test');
            const page1 = readPage(fd, 1, 'database.test');

            expect(
                header.page.toString(
                    'utf8',
                    MH_IDENTIFICATOR_POSITION,
                    MH_IDENTIFICATOR_POSITION + 3,
                ),
            ).toBe('CDB');
            expect(header.page.readUInt8(MH_VERSION_POSITION)).toBe(1);
            expect(header.page.readUInt16LE(MH_PAGE_SIZE_POSITION)).toBe(
                PAGE_SIZE,
            );
            expect(page1.pageType).toBe(PAGE_TYPES.CATALOG_TABLE);

            closeDatabase(fd);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('validates an existing file and does not reinitialize it', () => {
        const dbPath = createTempDbFile();

        try {
            const fd1 = initDatabase(dbPath, true);
            const nextPageBeforeClose = readPage(
                fd1,
                0,
                'database.test',
            ).page.readUInt32LE(7);
            closeDatabase(fd1);

            const fd2 = initDatabase(dbPath, false);
            const nextPageAfterReopen = readPage(
                fd2,
                0,
                'database.test',
            ).page.readUInt32LE(7);

            expect(nextPageAfterReopen).toBe(nextPageBeforeClose);
            closeDatabase(fd2);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('throws on invalid magic bytes in existing file', () => {
        const dbPath = createTempDbFile();

        try {
            const fd = fs.openSync(dbPath, 'w+');
            fs.writeSync(fd, Buffer.alloc(PAGE_SIZE), 0, PAGE_SIZE, 0);
            fs.closeSync(fd);

            expect(() => initDatabase(dbPath, false)).toThrow(StorageError);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });
});
