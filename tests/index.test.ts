import { describe, expect, expectTypeOf, it } from 'vitest';

import * as entry from '../src/index';
import type { PageType as ExportedPageType } from '../src/index';
import { initDatabase, closeDatabase } from '../src/database';
import {
    createTable,
    createColumn,
    isForeignKey,
    getTable,
    getColumn,
} from '../src/catalog';
import { allocatePage, loadPage, getLatestPage } from '../src/page';
import {
    createBitmapPage,
    createFixedPage,
    createSlottedPage,
} from '../src/data-pages';
import {
    PAGE_SIZE,
    PAGE_TYPES,
    DATA_TYPES,
    type PageType,
} from '../src/constants';
import { StorageError, ValidationError } from '../src/errors';

describe('index exports', () => {
    it('re-exports selected public APIs', () => {
        expect(entry.initDatabase).toBe(initDatabase);
        expect(entry.closeDatabase).toBe(closeDatabase);
        expect(entry.createTable).toBe(createTable);
        expect(entry.createColumn).toBe(createColumn);
        expect(entry.isForeignKey).toBe(isForeignKey);
        expect(entry.getTable).toBe(getTable);
        expect(entry.getColumn).toBe(getColumn);
        expect(entry.allocatePage).toBe(allocatePage);
        expect(entry.loadPage).toBe(loadPage);
        expect(entry.getLatestPage).toBe(getLatestPage);
        expect(entry.createBitmapPage).toBe(createBitmapPage);
        expect(entry.createFixedPage).toBe(createFixedPage);
        expect(entry.createSlottedPage).toBe(createSlottedPage);
        expect(entry.PAGE_SIZE).toBe(PAGE_SIZE);
        expect(entry.PAGE_TYPES).toBe(PAGE_TYPES);
        expect(entry.DATA_TYPES).toBe(DATA_TYPES);
        expect(entry.StorageError).toBe(StorageError);
        expect(entry.ValidationError).toBe(ValidationError);
        expectTypeOf<PageType>().toEqualTypeOf<ExportedPageType>();
    });
});
