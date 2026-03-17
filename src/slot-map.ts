import * as fs from 'fs';
import { allocatePage, readPage, getLatestPage } from './page';
import {
    PAGE_SIZE,
    NEXT_PAGE_ID_POSITION,
    NEXT_SLOT_OFFSET_POSITION,
    RECORD_COUNT_POSITION,
    PAGE_TYPES,
    SLOT_MAP_SLOT_SIZE,
    SLOT_MAP_SLOT,
    DELETED_SLOT,
} from './constants';
import {
    StorageErrorCode,
    StorageError,
    ValidationError,
    ValidationErrorCode,
} from './errors';

const writeSlotMapEntry = (
    fd: number,
    startingPageId: number,
    rowId: number,
    slotIndex: number,
) => {
    // find the last page (to add new entries)
    let pageId = getLatestPage(fd, startingPageId, PAGE_TYPES.SLOT_MAP);
    let slotMap = readPage(fd, pageId, 'writeSlotMapEntry');

    let nextOffset = slotMap.nextOffset;
    let nrEntries = slotMap.recordCount;

    // check if the page has space for a new 8-byte definition
    if (nextOffset + SLOT_MAP_SLOT_SIZE > PAGE_SIZE) {
        const newPageId = allocatePage(
            fd,
            PAGE_TYPES.SLOT_MAP,
            'writeSlotMapEntry',
        );

        // link the current page to the new one
        slotMap.page.writeUInt32LE(newPageId, NEXT_PAGE_ID_POSITION);

        const written = fs.writeSync(
            fd,
            slotMap.page,
            0,
            PAGE_SIZE,
            pageId * PAGE_SIZE,
        );

        if (written !== PAGE_SIZE) {
            throw new StorageError(
                StorageErrorCode.SHORT_WRITE,
                'Short Write while linking slot map pages',
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

        pageId = newPageId;

        slotMap = readPage(fd, pageId, 'writeSlotMapEntry');

        nextOffset = 16;
        nrEntries = 0;
    }

    slotMap.page.writeUInt32LE(rowId, nextOffset + SLOT_MAP_SLOT.ROW_ID);

    slotMap.page.writeUInt32LE(
        slotIndex,
        nextOffset + SLOT_MAP_SLOT.SLOT_INDEX,
    );

    // update metadata for this page
    slotMap.page.writeUInt16LE(
        nextOffset + SLOT_MAP_SLOT_SIZE,
        NEXT_SLOT_OFFSET_POSITION,
    );

    slotMap.page.writeUInt16LE(nrEntries + 1, RECORD_COUNT_POSITION);

    // write the page to disk
    const written = fs.writeSync(
        fd,
        slotMap.page,
        0,
        PAGE_SIZE,
        pageId * PAGE_SIZE,
    );

    if (written !== PAGE_SIZE) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing the slot map page',
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
};

const readSlotMapEntry = (
    fd: number,
    startingPageId: number,
    rowId: number,
) => {
    let parsedPage = readPage(fd, startingPageId, 'readSlotMapEntry');
    let offset = 16;
    let page = parsedPage.page;
    let nextPageId = parsedPage.nextPageId;
    let entryCount = parsedPage.recordCount;

    while (true) {
        for (let i = 0; i < entryCount; i++) {
            // check if the row id matches the row id we're searching for
            if (page.readUInt32LE(offset + SLOT_MAP_SLOT.ROW_ID) !== rowId) {
                offset += SLOT_MAP_SLOT_SIZE;
                continue;
            }

            const slotIndex = page.readUInt32LE(
                offset + SLOT_MAP_SLOT.SLOT_INDEX,
            );

            if (slotIndex === DELETED_SLOT) {
                throw new ValidationError(
                    ValidationErrorCode.BAD_INPUT,
                    'There exists no entry with the rowId ' + rowId,
                );
            }

            return {
                rowId,
                slotIndex,
            };
        }

        // if nextPageId = 0 that means that there are no more slot map pages
        // and that the entry with the specified rowId does not exist
        if (nextPageId === 0) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'There exists no entry with the rowId ' + rowId,
            );
        }

        // go to the next page
        parsedPage = readPage(fd, nextPageId, 'readSlotMapEntry');
        offset = 16;
        page = parsedPage.page;
        nextPageId = parsedPage.nextPageId;
        entryCount = parsedPage.recordCount;
    }
};

const deleteSlotMapEntry = (
    fd: number,
    startingPageId: number,
    rowId: number,
) => {
    let pageId = startingPageId;
    let parsedPage = readPage(fd, startingPageId, 'deleteSlotMapEntry');
    let offset = 16;
    let page = parsedPage.page;
    let nextPageId = parsedPage.nextPageId;
    let entryCount = parsedPage.recordCount;

    while (true) {
        for (let i = 0; i < entryCount; i++) {
            // check if the row id matches the row id we're searching for
            if (page.readUInt32LE(offset + SLOT_MAP_SLOT.ROW_ID) !== rowId) {
                offset += SLOT_MAP_SLOT_SIZE;
                continue;
            }

            page.writeUInt32LE(DELETED_SLOT, offset + SLOT_MAP_SLOT.SLOT_INDEX);

            const written = fs.writeSync(
                fd,
                page,
                0,
                PAGE_SIZE,
                pageId * PAGE_SIZE,
            );

            if (written !== PAGE_SIZE) {
                throw new StorageError(
                    StorageErrorCode.SHORT_WRITE,
                    'Short Write while writing the updated page to disk',
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

            return;
        }

        // if nextPageId = 0 that means that there are no more slot map pages
        // and that the entry with the specified rowId does not exist
        if (nextPageId === 0) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'There exists no entry with the rowId ' + rowId,
            );
        }

        // go to the next page
        pageId = nextPageId;
        parsedPage = readPage(fd, nextPageId, 'deleteSlotMapEntry');
        offset = 16;
        page = parsedPage.page;
        nextPageId = parsedPage.nextPageId;
        entryCount = parsedPage.recordCount;
    }
};

export { writeSlotMapEntry, readSlotMapEntry, deleteSlotMapEntry };
