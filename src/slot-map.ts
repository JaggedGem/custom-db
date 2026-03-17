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
} from './constants';
import {
    StorageErrorCode,
    StorageError,
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
