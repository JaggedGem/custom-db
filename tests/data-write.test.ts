import { describe, expect, it } from 'vitest';

import { getColumn, getTable } from '../src/catalog-read';
import { createTable } from '../src/catalog-write';
import {
    HEADER_SIZE,
    SLOT_DESCRIPTOR_SIZE,
} from '../src/constants';
import { closeDatabase, initDatabase } from '../src/database';
import { insertRow } from '../src/data-write';
import { ValidationError } from '../src/errors';
import { writeNullBit } from '../src/null-map';
import { readPage } from '../src/page';
import { readSlotMapEntry } from '../src/slot-map';
import { cleanupTempDbFile, createTempDbFile } from './helpers';

const insertRowUnchecked = (
    tableName: string,
    data: Record<string, string | number | boolean>,
    db: ReturnType<typeof initDatabase>,
) => insertRow(tableName, data as any, db);

const readBit = (page: Buffer, bitIndex: number) => {
    const byteOffset = HEADER_SIZE + Math.floor(bitIndex / 8);
    const bitMask = 1 << (bitIndex % 8);

    return (page.readUInt8(byteOffset) & bitMask) !== 0;
};

const readSlottedString = (page: Buffer, slotIndex: number) => {
    const descriptorOffset = HEADER_SIZE + slotIndex * SLOT_DESCRIPTOR_SIZE;
    const dataOffset = page.readUInt16LE(descriptorOffset);
    const dataLength = page.readUInt16LE(descriptorOffset + 2);

    return page.toString('utf8', dataOffset, dataOffset + dataLength);
};

describe('data-write/insertRow', () => {
    it('persists supported data types and updates table metadata, slot map and null map', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);

            createTable(db, 'users', [
                { name: 'id', type: 'integer' },
                { name: 'active', type: 'boolean' },
                { name: 'name', type: 'string' },
            ]);

            createTable(db, 'posts', [
                { name: 'id', type: 'integer' },
                {
                    name: 'author',
                    type: 'foreign_key',
                    foreignKey: { table: 'users', column: 'id' },
                },
                { name: 'title', type: 'string' },
            ]);

            const usersTable = getTable('users', db);

            // Pre-set null bits to true so insertRow must clear them for inserted rows.
            for (let colIndex = 0; colIndex < usersTable.numCols; colIndex++) {
                writeNullBit(
                    db.fd,
                    usersTable.masterNMapPageId,
                    0,
                    colIndex,
                    usersTable.numCols,
                    true,
                );

                writeNullBit(
                    db.fd,
                    usersTable.masterNMapPageId,
                    1,
                    colIndex,
                    usersTable.numCols,
                    true,
                );
            }

            const firstUserSlot = insertRowUnchecked(
                'users',
                { id: 1, active: true, name: 'Ada' },
                db,
            );

            const secondUserSlot = insertRowUnchecked(
                'users',
                { id: 2, active: false, name: 'Linus' },
                db,
            );

            expect(firstUserSlot).toBe(0);
            expect(secondUserSlot).toBe(1);

            db.tableCache.clear();
            const usersReloaded = getTable('users', db);
            expect(usersReloaded.nextRowId).toBe(2);

            const usersIdCol = getColumn('id', usersReloaded, db);
            const usersActiveCol = getColumn('active', usersReloaded, db);
            const usersNameCol = getColumn('name', usersReloaded, db);

            const idPage = readPage(db.fd, usersIdCol.columnDataId, 'data-write.test');
            expect(idPage.recordCount).toBe(2);
            expect(idPage.page.readInt32LE(HEADER_SIZE)).toBe(1);
            expect(idPage.page.readInt32LE(HEADER_SIZE + 4)).toBe(2);

            const activePage = readPage(
                db.fd,
                usersActiveCol.columnDataId,
                'data-write.test',
            );

            expect(activePage.recordCount).toBe(2);
            expect(readBit(activePage.page, 0)).toBe(true);
            expect(readBit(activePage.page, 1)).toBe(false);

            const namePage = readPage(db.fd, usersNameCol.columnDataId, 'data-write.test');
            expect(namePage.recordCount).toBe(2);
            expect(readSlottedString(namePage.page, 0)).toBe('Ada');
            expect(readSlottedString(namePage.page, 1)).toBe('Linus');

            expect(readSlotMapEntry(db.fd, usersReloaded.slotMapId, 0)).toEqual({
                rowId: 0,
                slotIndex: 0,
            });

            expect(readSlotMapEntry(db.fd, usersReloaded.slotMapId, 1)).toEqual({
                rowId: 1,
                slotIndex: 1,
            });

            const nullMapPage = readPage(
                db.fd,
                usersReloaded.masterNMapPageId,
                'data-write.test',
            );

            for (let bit = 0; bit < usersReloaded.numCols * 2; bit++) {
                expect(readBit(nullMapPage.page, bit)).toBe(false);
            }

            const firstPostSlot = insertRowUnchecked(
                'posts',
                { id: 11, author: 1, title: 'Hello CDB' },
                db,
            );

            expect(firstPostSlot).toBe(0);

            db.tableCache.clear();
            const postsReloaded = getTable('posts', db);
            expect(postsReloaded.nextRowId).toBe(1);

            const postsAuthorCol = getColumn('author', postsReloaded, db);
            const postsTitleCol = getColumn('title', postsReloaded, db);

            const authorPage = readPage(
                db.fd,
                postsAuthorCol.columnDataId,
                'data-write.test',
            );

            expect(authorPage.recordCount).toBe(1);
            expect(authorPage.page.readUInt32LE(HEADER_SIZE)).toBe(1);

            const titlePage = readPage(db.fd, postsTitleCol.columnDataId, 'data-write.test');
            expect(titlePage.recordCount).toBe(1);
            expect(readSlottedString(titlePage.page, 0)).toBe('Hello CDB');

            expect(readSlotMapEntry(db.fd, postsReloaded.slotMapId, 0)).toEqual({
                rowId: 0,
                slotIndex: 0,
            });

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('rejects rows whose number of values does not match table columns', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);

            createTable(db, 'users', [
                { name: 'id', type: 'integer' },
                { name: 'active', type: 'boolean' },
            ]);

            expect(() =>
                insertRow('users', { id: 1 } as any, db),
            ).toThrow(ValidationError);

            expect(() =>
                insertRow(
                    'users',
                    { id: 1, active: true, extra: 123 } as any,
                    db,
                ),
            ).toThrow(ValidationError);

            const usersTable = getTable('users', db);
            expect(usersTable.nextRowId).toBe(0);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('rejects unknown columns and invalid value types', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);

            createTable(db, 'users', [
                { name: 'id', type: 'integer' },
                { name: 'active', type: 'boolean' },
            ]);

            createTable(db, 'posts', [
                { name: 'id', type: 'integer' },
                {
                    name: 'author',
                    type: 'foreign_key',
                    foreignKey: { table: 'users', column: 'id' },
                },
                { name: 'title', type: 'string' },
            ]);

            expect(() =>
                insertRow('users', { id: 1, missing: false } as any, db),
            ).toThrow(ValidationError);

            expect(() =>
                insertRow('users', { id: 'bad', active: true } as any, db),
            ).toThrow(ValidationError);

            expect(() =>
                insertRow('users', { id: 1, active: 'bad' } as any, db),
            ).toThrow(ValidationError);

            expect(() =>
                insertRow(
                    'posts',
                    { id: 1, author: 'bad', title: 'invalid fk' } as any,
                    db,
                ),
            ).toThrow(ValidationError);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });
});