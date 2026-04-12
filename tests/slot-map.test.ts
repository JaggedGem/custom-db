import fs from 'node:fs';
import { describe, expect, it } from 'vitest';

import {
    HEADER_SIZE,
    PAGE_SIZE,
    PAGE_TYPES,
    NEXT_PAGE_ID_POSITION,
    NEXT_SLOT_OFFSET_POSITION,
    RECORD_COUNT_POSITION,
    SLOT_MAP_SLOT,
    SLOT_MAP_SLOT_SIZE,
    DELETED_SLOT,
} from '../src/constants';
import { closeDatabase, initDatabase } from '../src/database';
import { allocatePage, readPage } from '../src/page';
import {
    deleteSlotMapEntry,
    readSlotMapEntry,
    writeSlotMapEntry,
} from '../src/slot-map';
import { cleanupTempDbFile, createTempDbFile } from './helpers';
import { ValidationError } from '../src/errors';

describe('slot-map', () => {
    it('writeSlotMapEntry writes entry and updates metadata', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const slotMapPageId = allocatePage(
                fd,
                PAGE_TYPES.SLOT_MAP,
                'slot-map.test',
            );

            writeSlotMapEntry(fd, slotMapPageId, 42, 7);

            const page = readPage(fd, slotMapPageId, 'slot-map.test');
            expect(page.recordCount).toBe(1);
            expect(page.nextOffset).toBe(HEADER_SIZE + SLOT_MAP_SLOT_SIZE);
            expect(
                page.page.readUInt32LE(HEADER_SIZE + SLOT_MAP_SLOT.ROW_ID),
            ).toBe(42);
            expect(
                page.page.readUInt32LE(HEADER_SIZE + SLOT_MAP_SLOT.SLOT_INDEX),
            ).toBe(7);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('writeSlotMapEntry allocates a new page when current page is full', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const slotMapPageId = allocatePage(
                fd,
                PAGE_TYPES.SLOT_MAP,
                'slot-map.test',
            );

            const fullPage = readPage(fd, slotMapPageId, 'slot-map.test').page;
            fullPage.writeUInt16LE(
                PAGE_SIZE - SLOT_MAP_SLOT_SIZE + 1,
                NEXT_SLOT_OFFSET_POSITION,
            );
            fullPage.writeUInt16LE(1, RECORD_COUNT_POSITION);
            fs.writeSync(fd, fullPage, 0, PAGE_SIZE, slotMapPageId * PAGE_SIZE);
            fs.fsyncSync(fd);

            writeSlotMapEntry(fd, slotMapPageId, 99, 123);

            const firstPage = readPage(fd, slotMapPageId, 'slot-map.test');
            const nextPageId = firstPage.page.readUInt32LE(
                NEXT_PAGE_ID_POSITION,
            );
            expect(nextPageId).not.toBe(0);

            const secondPage = readPage(fd, nextPageId, 'slot-map.test');
            expect(secondPage.pageType).toBe(PAGE_TYPES.SLOT_MAP);
            expect(secondPage.recordCount).toBe(1);
            expect(secondPage.nextOffset).toBe(
                HEADER_SIZE + SLOT_MAP_SLOT_SIZE,
            );
            expect(
                secondPage.page.readUInt32LE(
                    HEADER_SIZE + SLOT_MAP_SLOT.ROW_ID,
                ),
            ).toBe(99);
            expect(
                secondPage.page.readUInt32LE(
                    HEADER_SIZE + SLOT_MAP_SLOT.SLOT_INDEX,
                ),
            ).toBe(123);

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('readSlotMapEntry retrieves the slot index for the requested row', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const slotMapPageId = allocatePage(
                fd,
                PAGE_TYPES.SLOT_MAP,
                'slot-map.test',
            );

            writeSlotMapEntry(fd, slotMapPageId, 10, 3);
            writeSlotMapEntry(fd, slotMapPageId, 20, 4);

            const entry = readSlotMapEntry(fd, slotMapPageId, 20);
            expect(entry).toEqual({ rowId: 20, slotIndex: 4 });

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('readSlotMapEntry throws when row does not exist', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const slotMapPageId = allocatePage(
                fd,
                PAGE_TYPES.SLOT_MAP,
                'slot-map.test',
            );

            writeSlotMapEntry(fd, slotMapPageId, 5, 8);

            expect(() => readSlotMapEntry(fd, slotMapPageId, 6)).toThrow(
                ValidationError,
            );

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('deleteSlotMapEntry marks the entry as deleted and readSlotMapEntry rejects it', () => {
        const dbPath = createTempDbFile();

        try {
            const db = initDatabase(dbPath, true);
            const fd = db.fd;
            const slotMapPageId = allocatePage(
                fd,
                PAGE_TYPES.SLOT_MAP,
                'slot-map.test',
            );

            writeSlotMapEntry(fd, slotMapPageId, 7, 9);
            deleteSlotMapEntry(fd, slotMapPageId, 7);

            const page = readPage(fd, slotMapPageId, 'slot-map.test');
            expect(
                page.page.readUInt32LE(HEADER_SIZE + SLOT_MAP_SLOT.SLOT_INDEX),
            ).toBe(DELETED_SLOT);
            expect(() => readSlotMapEntry(fd, slotMapPageId, 7)).toThrow(
                ValidationError,
            );

            closeDatabase(db);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });
});
