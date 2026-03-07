import { describe, expect, it } from 'vitest';

import {
    COLUMN_SLOT_SIZE,
    DATA_TYPES,
    MH_IDENTIFICATOR_POSITION,
    MH_NEXT_FREE_PAGE_ID_POSITION,
    MH_PAGE_SIZE_POSITION,
    MH_VERSION_POSITION,
    NEXT_PAGE_ID_POSITION,
    NEXT_SLOT_OFFSET_POSITION,
    PAGE_SIZE,
    PAGE_TYPES,
    PAGE_TYPE_POSITION,
    RECORD_COUNT_POSITION,
    TABLE_SLOT_SIZE,
} from '../src/constants';

describe('constants', () => {
    it('exposes expected master header offsets and page size', () => {
        expect(PAGE_SIZE).toBe(4096);
        expect(MH_IDENTIFICATOR_POSITION).toBe(0);
        expect(MH_VERSION_POSITION).toBe(4);
        expect(MH_PAGE_SIZE_POSITION).toBe(5);
        expect(MH_NEXT_FREE_PAGE_ID_POSITION).toBe(7);
    });

    it('exposes expected page header offsets and slot sizes', () => {
        expect(NEXT_PAGE_ID_POSITION).toBe(0);
        expect(NEXT_SLOT_OFFSET_POSITION).toBe(4);
        expect(RECORD_COUNT_POSITION).toBe(6);
        expect(PAGE_TYPE_POSITION).toBe(15);
        expect(TABLE_SLOT_SIZE).toBe(64);
        expect(COLUMN_SLOT_SIZE).toBe(48);
    });

    it('exposes page type and data type mappings', () => {
        expect(PAGE_TYPES.MASTER_NULL_MAP).toBe(1);
        expect(PAGE_TYPES.CATALOG_TABLE).toBe(2);
        expect(PAGE_TYPES.CATALOG_COLUMN).toBe(3);
        expect(PAGE_TYPES.DATA_SLOTTED).toBe(4);
        expect(PAGE_TYPES.DATA_FIXED).toBe(5);
        expect(PAGE_TYPES.DATA_BITMAP).toBe(6);

        expect(DATA_TYPES.BOOLEAN).toBe(1);
        expect(DATA_TYPES.INTEGER).toBe(2);
        expect(DATA_TYPES.STRING).toBe(3);
        expect(DATA_TYPES.FOREIGN_KEY).toBe(4);
    });
});
