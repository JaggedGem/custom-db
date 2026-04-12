import * as fs from 'fs';
import {
    PAGE_SIZE,
    NEXT_PAGE_ID_POSITION,
    NEXT_SLOT_OFFSET_POSITION,
    RECORD_COUNT_POSITION,
    PAGE_TYPE_POSITION,
    PAGE_TYPES,
    HEADER_SIZE,
    PageType,
} from './constants';
import {
    StorageErrorCode,
    StorageError,
    ValidationErrorCode,
    ValidationError,
} from './errors';
import { pageType } from './types';

const writeHeader = (
    fd: number,
    pageId: number,
    pageType: PageType,
    caller: string,
) => {
    const buf = Buffer.alloc(PAGE_SIZE);

    // Header Structure
    buf.writeUInt32LE(0, NEXT_PAGE_ID_POSITION); // next page id (0 = end of chain)
    buf.writeUInt16LE(HEADER_SIZE, NEXT_SLOT_OFFSET_POSITION); // next offset (start after header)
    buf.writeUInt16LE(0, RECORD_COUNT_POSITION); // record count (0)
    // ... bytes 8-14 are padding ...
    buf.writeUInt8(pageType, PAGE_TYPE_POSITION); // page type identifier (refer to PAGE_TYPES)

    const written = fs.writeSync(fd, buf, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing page header',
            {
                context: {
                    caller,
                    pageId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written,
                    position: pageId * PAGE_SIZE,
                },
            },
        );
    }

    fs.fsyncSync(fd);

    return buf;
};

const readPage = (fd: number, pageId: number, caller: string): pageType => {
    const buf = Buffer.alloc(PAGE_SIZE);
    const bytes = fs.readSync(fd, buf, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    if (bytes !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_READ,
            'Short Read while reading page',
            {
                context: {
                    caller,
                    pageId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: bytes,
                    position: pageId * PAGE_SIZE,
                },
            },
        );
    }

    return {
        nextPageId: buf.readUInt32LE(NEXT_PAGE_ID_POSITION),
        nextOffset: buf.readUInt16LE(NEXT_SLOT_OFFSET_POSITION),
        recordCount: buf.readUInt16LE(RECORD_COUNT_POSITION),
        pageType: buf.readUInt8(PAGE_TYPE_POSITION),
        page: buf,
    };
};

const verifyPageType = (
    fd: number,
    pageId: number,
    caller: string,
    expectedType: PageType,
) => {
    const { pageType } = readPage(fd, pageId, 'verifyPageType/' + caller);
    return expectedType === pageType;
};

const allocatePage = (fd: number, pageType: PageType, caller: string) => {
    if (!Object.values(PAGE_TYPES).includes(pageType)) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'Invalid Page Type',
            {
                context: {
                    caller,
                    expectedValue: Object.values(PAGE_TYPES),
                    actualValue: pageType,
                },
            },
        );
    }

    // Use readPage so the master header read is checked for short reads
    const { page: header } = readPage(fd, 0, 'allocatePage/' + caller);

    const nextId = header.readUInt32LE(7);

    const newPage = Buffer.alloc(PAGE_SIZE);
    const written = fs.writeSync(fd, newPage, 0, PAGE_SIZE, nextId * PAGE_SIZE);
    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while allocating new page',
            {
                context: {
                    caller,
                    pageId: nextId,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written,
                    position: nextId * PAGE_SIZE,
                },
            },
        );
    }

    writeHeader(fd, nextId, pageType, 'allocatePage/' + caller);

    header.writeUInt32LE(nextId + 1, 7);
    const written1 = fs.writeSync(fd, header, 0, PAGE_SIZE, 0);
    if (written1 !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing new next page id',
            {
                context: {
                    caller,
                    pageId: 0,
                    expectedBytes: PAGE_SIZE,
                    actualBytes: written1,
                    position: 0,
                },
            },
        );
    }

    fs.fsyncSync(fd);

    return nextId;
};

const loadPage = (fd: number, pageId: number, caller: string) => {
    return readPage(fd, pageId, caller).page;
};

function getLatestPage(
    fd: number,
    startingPageId: number,
    expectedType: PageType,
) {
    let currentPageId = startingPageId;

    while (true) {
        // Single read per iteration — use pageType directly instead of a
        // second verifyPageType call which would re-read the same page.
        const page = readPage(fd, currentPageId, 'getLatestPage');

        if (page.pageType !== expectedType) {
            throw new StorageError(
                StorageErrorCode.IO_ERROR,
                'Pages in chain should have the same page types',
                {
                    context: {
                        pageId: currentPageId,
                        expectedType,
                        actualType: page.pageType,
                    },
                },
            );
        }

        // If nextPageId is 0, this IS the last page in the chain
        if (page.nextPageId === 0) {
            return currentPageId;
        }

        // Otherwise, move to the next page
        currentPageId = page.nextPageId;
    }
}

export {
    writeHeader,
    readPage,
    verifyPageType,
    allocatePage,
    getLatestPage,
    loadPage,
};
