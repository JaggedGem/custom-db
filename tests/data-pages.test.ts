import { describe, expect, it } from 'vitest';

import { PAGE_SIZE, PAGE_TYPES } from '../src/constants';
import { closeDatabase, initDatabase } from '../src/database';
import {
    createBitmapPage,
    createFixedPage,
    createSlottedPage,
} from '../src/data-pages';
import { readPage } from '../src/page';
import { cleanupTempDbFile, createTempDbFile } from './helpers';

describe('data-pages', () => {
    it('createBitmapPage initializes bitmap-specific field', () => {
        const dbPath = createTempDbFile();

        try {
            const fd = initDatabase(dbPath, true);
            const pageId = createBitmapPage(fd);
            const page = readPage(fd, pageId, 'data-pages.test');

            expect(page.pageType).toBe(PAGE_TYPES.DATA_BITMAP);
            expect(page.page.readUInt32LE(8)).toBe(0);

            closeDatabase(fd);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('createFixedPage allocates a fixed data page', () => {
        const dbPath = createTempDbFile();

        try {
            const fd = initDatabase(dbPath, true);
            const pageId = createFixedPage(fd);
            const page = readPage(fd, pageId, 'data-pages.test');

            expect(page.pageType).toBe(PAGE_TYPES.DATA_FIXED);

            closeDatabase(fd);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });

    it('createSlottedPage sets free-space pointer to end of page', () => {
        const dbPath = createTempDbFile();

        try {
            const fd = initDatabase(dbPath, true);
            const pageId = createSlottedPage(fd);
            const page = readPage(fd, pageId, 'data-pages.test');

            expect(page.pageType).toBe(PAGE_TYPES.DATA_SLOTTED);
            expect(page.page.readUInt16LE(12)).toBe(PAGE_SIZE);

            closeDatabase(fd);
        } finally {
            cleanupTempDbFile(dbPath);
        }
    });
});
