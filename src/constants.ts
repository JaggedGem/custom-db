const PAGE_SIZE = 4096;
const DELETED_SLOT = 0xffffffff;

// master header
const MH_IDENTIFICATOR_POSITION = 0;
const MH_VERSION_POSITION = 4;
const MH_PAGE_SIZE_POSITION = 5;
const MH_NEXT_FREE_PAGE_ID_POSITION = 7;

// base page headers
const NEXT_PAGE_ID_POSITION = 0;
const NEXT_SLOT_OFFSET_POSITION = 4;
const RECORD_COUNT_POSITION = 6;
const PAGE_TYPE_POSITION = 15;

// slotted page specific headers
const FREE_SPACE_POINTER_POSITION = 12;

// slot sizes
const TABLE_SLOT_SIZE = 64;
const COLUMN_SLOT_SIZE = 48;
const SLOT_MAP_SLOT_SIZE = 8;
const HEADER_SIZE = 16;
const SLOT_DESCRIPTOR_SIZE = 4;

const BITS_PER_PAGE = (PAGE_SIZE - HEADER_SIZE) * 8;

const PAGE_TYPES = {
    MASTER_NULL_MAP: 1,
    CATALOG_TABLE: 2,
    CATALOG_COLUMN: 3,
    DATA_SLOTTED: 4,
    DATA_FIXED: 5,
    DATA_BITMAP: 6,
    SLOT_MAP: 7,
} as const;

const DATA_TYPES = {
    BOOLEAN: 1,
    INTEGER: 2,
    STRING: 3,
    FOREIGN_KEY: 4,
};

const DATA_TYPE_LOOKUP = {
    1: 'boolean',
    2: 'integer',
    3: 'string',
    4: 'foreign_key',
} as const;

// Non-FK:  name(12) + type(1) + dataPageId(4) + pad(31) = 48
// FK:      name(12) + type(1) + fkTable(12) + fkCol(12) + fkRefPageId(4) + dataPageId(4) + pad(3) = 48

const COL_SLOT = {
    NAME: 0,
    TYPE: 12,
    DATA_PAGE_ID: 13, // non-FK
    FK_TABLE: 13, // FK
    FK_COL: 25, // FK
    FK_REF_PAGE_ID: 37, // FK
    FK_DATA_PAGE_ID: 41, // FK
} as const;

const TABLE_SLOT = {
    NAME: 0,
    MASTER_NMAP_PAGE_ID: 12,
    NEXT_ROW_ID: 16,
    SLOT_MAP: 20,
    COL_DEFS: 24,
    NUM_COLS: 28,
} as const;

const SLOT_MAP_SLOT = {
    ROW_ID: 0,
    SLOT_INDEX: 4,
} as const;

export type PageType = (typeof PAGE_TYPES)[keyof typeof PAGE_TYPES];

export {
    MH_IDENTIFICATOR_POSITION,
    MH_NEXT_FREE_PAGE_ID_POSITION,
    MH_PAGE_SIZE_POSITION,
    MH_VERSION_POSITION,
    PAGE_SIZE,
    NEXT_PAGE_ID_POSITION,
    NEXT_SLOT_OFFSET_POSITION,
    RECORD_COUNT_POSITION,
    PAGE_TYPE_POSITION,
    PAGE_TYPES,
    DATA_TYPES,
    TABLE_SLOT_SIZE,
    COLUMN_SLOT_SIZE,
    DELETED_SLOT,
    COL_SLOT,
    TABLE_SLOT,
    DATA_TYPE_LOOKUP,
    SLOT_MAP_SLOT_SIZE,
    SLOT_MAP_SLOT,
    HEADER_SIZE,
    FREE_SPACE_POINTER_POSITION,
    SLOT_DESCRIPTOR_SIZE,
    BITS_PER_PAGE,
};
