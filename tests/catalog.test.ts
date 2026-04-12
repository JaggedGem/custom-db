import { describe, expect, it } from 'vitest';

import {
    COL_SLOT,
    COLUMN_SLOT_SIZE,
    DATA_TYPES,
    HEADER_SIZE,
    PAGE_TYPES,
    TABLE_SLOT_SIZE,
} from '../src/constants';
import { closeDatabase, initDatabase } from '../src/database';
import {
    createColumn,
    createTable,
} from '../src/catalog-write';
import { allocatePage, loadPage, readPage } from '../src/page';
import { ValidationError } from '../src/errors';
import { isForeignKey, type Column, type Table } from '../src/types';
import { cleanupTempDbFile, createTempDbFile } from './helpers';
import { getColumn, getTable } from '../src/catalog-read';

describe('catalog', () => {
    it('isForeignKey narrows union correctly', () => {
        const fk: Column = {
            name: 'user_id',
            type: 'foreign_key',
            foreignKey: { table: 'users', column: 'id' },
        };

        const normal: Column = { name: 'age', type: 'integer' };

        expect(isForeignKey(fk)).toBe(true);
        if (isForeignKey(fk)) {
            expect(fk.foreignKey.table).toBe('users');
        }
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

            const dataPageId = createColumn(db, colDefsPageId, {
                name: 'age',
                type: 'integer',
            });

            const colPage = readPage(fd, colDefsPageId, 'catalog.test');
            expect(colPage.recordCount).toBe(1);
            expect(colPage.nextOffset).toBe(HEADER_SIZE + COLUMN_SLOT_SIZE);
            const slotOffset = HEADER_SIZE;
            expect(colPage.page.readUInt8(slotOffset + COL_SLOT.TYPE)).toBe(
                DATA_TYPES.INTEGER,
            );
            expect(
                colPage.page.readUInt32LE(slotOffset + COL_SLOT.DATA_PAGE_ID),
            ).toBe(dataPageId);

            const dataPage = readPage(fd, dataPageId, 'catalog.test');
            expect(dataPage.pageType).toBe(PAGE_TYPES.DATA_FIXED);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('createColumn rejects bad input and missing foreign key info', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const colDefsPageId = allocatePage(
                fd,
                PAGE_TYPES.CATALOG_COLUMN,
                'catalog.test',
            );

            expect(() =>
                createColumn(db, colDefsPageId, {
                    name: 'column_name_too_long',
                    type: 'integer',
                }),
            ).toThrow(ValidationError);

            expect(() =>
                createColumn(db, colDefsPageId, {
                    name: 'fk',
                    type: 'foreign_key',
                    foreignKey: { table: '', column: 'id' },
                }),
            ).toThrow(ValidationError);

            expect(() =>
                createColumn(db, colDefsPageId, {
                    name: 'fk',
                    type: 'foreign_key',
                    foreignKey: { table: 'users', column: '' },
                }),
            ).toThrow(ValidationError);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('createTable writes table entry, caches it and getTable retrieves it', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);

            createTable(db, 'users', [
                { name: 'id', type: 'integer' },
                { name: 'name', type: 'string' },
            ]);

            const tableCatalogPage = readPage(db.fd, 1, 'catalog.test');
            expect(tableCatalogPage.recordCount).toBe(1);
            expect(tableCatalogPage.nextOffset).toBe(
                HEADER_SIZE + TABLE_SLOT_SIZE,
            );

            const table = getTable('users', db);
            expect(table.name).toBe('users');
            expect(table.masterNMapPageId).toBeGreaterThan(1);
            expect(table.colDefsPageId).toBeGreaterThan(1);
            expect(table.nextRowId).toBe(0);
            expect(table.slotMapId).toBeGreaterThan(1);

            const cached = getTable('users', db);
            expect(cached).toStrictEqual(table);

            const colDefsPage = loadPage(
                db.fd,
                table.colDefsPageId,
                'catalog.test',
            );
            expect(colDefsPage.readUInt16LE(6)).toBe(2);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('getColumn resolves column metadata and caches lookups', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            createTable(db, 'users', [
                { name: 'id', type: 'integer' },
                { name: 'age', type: 'integer' },
            ]);

            const table: Table = getTable('users', db);

            const first = getColumn('id', table, db);
            expect(first.type).toBe('integer');
            expect(first.columnDataId).toBeGreaterThan(1);

            const second = getColumn('id', table, db);
            expect(second).toStrictEqual(first);

            expect(() => getColumn('missing', table, db)).toThrow(
                ValidationError,
            );

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('createTable supports foreign keys to normal columns', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);

            createTable(db, 'users', [{ name: 'id', type: 'integer' }]);
            createTable(db, 'posts', [
                { name: 'id', type: 'integer' },
                {
                    name: 'author',
                    type: 'foreign_key',
                    foreignKey: { table: 'users', column: 'id' },
                },
            ]);

            const usersTable = getTable('users', db);
            const postsTable = getTable('posts', db);

            const usersId = getColumn('id', usersTable, db);
            const postsAuthor = getColumn('author', postsTable, db);

            expect(postsAuthor.type).toBe('foreign_key');
            if (postsAuthor.type === 'foreign_key') {
                expect(postsAuthor.foreignKey.refPageId).toBe(
                    usersId.columnDataId,
                );
            }

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('createTable and getTable validate bad inputs', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);

            expect(() =>
                createTable(db, 'this_name_is_too_long', []),
            ).toThrow(ValidationError);

            createTable(db, 'users', []);

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
