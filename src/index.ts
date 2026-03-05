export { initDatabase, closeDatabase } from './database';
export { createTable, createColumn, isForeignKey } from './catalog';
export { allocatePage, loadPage, getLatestPage, writeHeader } from './page'; // todo: remove writeHeader from exports as it's an internal function
export {
    createBitmapPage,
    createFixedPage,
    createSlottedPage,
} from './data-pages';
export { PAGE_SIZE, PAGE_TYPES, DATA_TYPES, type PageType } from './constants';
export { StorageError, ValidationError } from './errors';
export type { Column } from './types';
