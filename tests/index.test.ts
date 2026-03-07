import { describe, expect, it } from 'vitest';

import * as entry from '../src/index';
import { initDatabase, closeDatabase } from '../src/database';
import { createTable } from '../src/catalog';
import { allocatePage } from '../src/page';
import { createBitmapPage } from '../src/data-pages';
import { PAGE_SIZE } from '../src/constants';
import { StorageError } from '../src/errors';

describe('index exports', () => {
    it('re-exports selected public APIs', () => {
        expect(entry.initDatabase).toBe(initDatabase);
        expect(entry.closeDatabase).toBe(closeDatabase);
        expect(entry.createTable).toBe(createTable);
        expect(entry.allocatePage).toBe(allocatePage);
        expect(entry.createBitmapPage).toBe(createBitmapPage);
        expect(entry.PAGE_SIZE).toBe(PAGE_SIZE);
        expect(entry.StorageError).toBe(StorageError);
    });
});
