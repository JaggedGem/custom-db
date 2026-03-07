import { describe, expect, it } from 'vitest';

import {
    COLUMN_SLOT_SIZE,
    DATA_TYPES,
    PAGE_TYPES,
    TABLE_SLOT_SIZE,
} from '../src/constants';
import { closeDatabase, initDatabase } from '../src/database';
import {
    createColumn,
    createTable,
    getTable,
    isForeignKey,
} from '../src/catalog';
import { allocatePage, loadPage, readPage, writeHeader } from '../src/page';
import { ValidationError } from '../src/errors';
import type { Column } from '../src/types';
import { cleanupTempDbFile, createTempDbFile } from './helpers';

describe('catalog', () => {
    it('isForeignKey narrows union correctly', () => {
        const fk: Column = {
            name: 'user_id',
            isForeignKey: true,
            foreignKey: { table: 'users', column: 'id' },
        };

        const normal: Column = {
            name: 'age',
            type: 'integer',
            isForeignKey: false,
        };

        expect(isForeignKey(fk)).toBe(true);
        expect(isForeignKey(normal)).toBe(false);
    });

    it('createColumn writes normal column metadata and allocates data page', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const colDefsPageId = allocatePage(
                fd,
                PAGE_TYPES.CATALOG_COLUMN,
                'catalog.test',
            );
            writeHeader(
                fd,
                colDefsPageId,
                PAGE_TYPES.CATALOG_COLUMN,
                'catalog.test',
            );

            const dataPageId = createColumn(fd, colDefsPageId, {
                name: 'age',
                type: 'integer',
                isForeignKey: false,
            });

            const colPage = readPage(fd, colDefsPageId, 'catalog.test');
            expect(colPage.recordCount).toBe(1);
            expect(colPage.nextOffset).toBe(16 + COLUMN_SLOT_SIZE);
            expect(colPage.page.readUInt8(16 + 12)).toBe(DATA_TYPES.INTEGER);
            expect(colPage.page.readUInt32LE(16 + 13)).toBe(dataPageId);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('createColumn writes foreign key metadata', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const colDefsPageId = allocatePage(
                fd,
                PAGE_TYPES.CATALOG_COLUMN,
                'catalog.test',
            );
            writeHeader(
                fd,
                colDefsPageId,
                PAGE_TYPES.CATALOG_COLUMN,
                'catalog.test',
            );

            createColumn(fd, colDefsPageId, {
                name: 'user_id',
                isForeignKey: true,
                foreignKey: { table: 'users', column: 'id' },
            });

            const colPage = readPage(fd, colDefsPageId, 'catalog.test');
            const slotOffset = 16;
            expect(colPage.page.readUInt8(slotOffset + 12)).toBe(
                DATA_TYPES.FOREIGN_KEY,
            );
            expect(
                colPage.page
                    .toString('utf8', slotOffset + 17, slotOffset + 29)
                    .replace(/\0+$/, ''),
            ).toBe('users');
            expect(
                colPage.page
                    .toString('utf8', slotOffset + 29, slotOffset + 41)
                    .replace(/\0+$/, ''),
            ).toBe('id');

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('createColumn rejects too-long column names and FK metadata names', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const colDefsPageId = allocatePage(
                fd,
                PAGE_TYPES.CATALOG_COLUMN,
                'catalog.test',
            );
            writeHeader(
                fd,
                colDefsPageId,
                PAGE_TYPES.CATALOG_COLUMN,
                'catalog.test',
            );

            expect(() =>
                createColumn(fd, colDefsPageId, {
                    name: 'column_name_too_long',
                    type: 'integer',
                    isForeignKey: false,
                }),
            ).toThrow(ValidationError);

            expect(() =>
                createColumn(fd, colDefsPageId, {
                    name: 'fk',
                    isForeignKey: true,
                    foreignKey: {
                        table: 'table_name_too_long',
                        column: 'id',
                    },
                }),
            ).toThrow(ValidationError);

            expect(() =>
                createColumn(fd, colDefsPageId, {
                    name: 'fk',
                    isForeignKey: true,
                    foreignKey: {
                        table: 'users',
                        column: 'column_name_too_long',
                    },
                }),
            ).toThrow(ValidationError);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('createTable writes table entry and getTable finds it', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;

            createTable(fd, 'users', [
                { name: 'id', type: 'integer', isForeignKey: false },
                { name: 'name', type: 'string', isForeignKey: false },
            ]);

            const tableCatalogPage = readPage(fd, 1, 'catalog.test');
            expect(tableCatalogPage.recordCount).toBe(1);
            expect(tableCatalogPage.nextOffset).toBe(16 + TABLE_SLOT_SIZE);

            const table = getTable('users', db);
            expect(table.name).toBe('users');
            expect(table.masterNMapPageId).toBeGreaterThan(1);
            expect(table.colDefsPageId).toBeGreaterThan(1);

            const colDefsPage = loadPage(
                fd,
                table.colDefsPageId,
                'catalog.test',
            );
            expect(colDefsPage.readUInt16LE(6)).toBe(2);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('createTable and getTable validate bad inputs', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;

            expect(() => createTable(fd, 'this_name_is_too_long', [])).toThrow(
                ValidationError,
            );

            createTable(fd, 'users', []);

            expect(() => getTable('this_name_is_too_long', db)).toThrow(
                ValidationError,
            );
            expect(() => getTable('missing', db)).toThrow(ValidationError);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });
});
