export { initDatabase, closeDatabase } from './database';
export {
    createTable,
    createColumn,
} from './catalog-write';
export {
    getTable,
    getColumn,
} from './catalog-read'
export { allocatePage, loadPage, getLatestPage } from './page';
export {
    createBitmapPage,
    createFixedPage,
    createSlottedPage,
} from './data-pages';
export { isForeignKey } from './types'
export { PAGE_SIZE, PAGE_TYPES, DATA_TYPES, type PageType } from './constants';
export { StorageError, ValidationError } from './errors';
export type { Column } from './types';
