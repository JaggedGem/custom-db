import * as fs from 'fs';
import { allocatePage, readPage } from './page';
import {
    PAGE_SIZE,
    PAGE_TYPES,
} from './constants';
import { StorageErrorCode, StorageError } from './errors';

const createBitmapPage = (fd: number) => {
    const pageId = allocatePage(fd, PAGE_TYPES.DATA_BITMAP, 'createBitmapPage');

    const { page } = readPage(fd, pageId, 'createBitmapPage');
    page.writeUInt32LE(0, 8); // total bits (bitmap-specific field)

    const written = fs.writeSync(fd, page, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing bitmap page',
            {
                context: {
                    pageId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written,
                    position: pageId * PAGE_SIZE,
                },
            },
        );
    }
    fs.fsyncSync(fd);

    return pageId;
};

const createFixedPage = (fd: number) => {
    const pageId = allocatePage(fd, PAGE_TYPES.DATA_FIXED, 'createFixedPage');

    return pageId;
};

const createSlottedPage = (fd: number) => {
    const pageId = allocatePage(
        fd,
        PAGE_TYPES.DATA_SLOTTED,
        'createSlottedPage',
    );

    const page = readPage(fd, pageId, 'createSlottedPage');
    page.page.writeUInt16LE(PAGE_SIZE, 12); // free space pointer (data grows from end of page)
    // ... byte 14 is reserved ...

    const written = fs.writeSync(fd, page.page, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing slotted page',
            {
                context: {
                    pageId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written,
                    position: pageId * PAGE_SIZE,
                },
            },
        );
    }
    fs.fsyncSync(fd);

    return pageId;
};

export { createBitmapPage, createFixedPage, createSlottedPage };
