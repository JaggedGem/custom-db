import {
    BITS_PER_PAGE,
    NEXT_PAGE_ID_POSITION,
    PAGE_SIZE,
    PAGE_TYPES,
} from './constants';
import { StorageError, StorageErrorCode } from './errors';
import { allocatePage, readPage } from './page';
import { setBit } from './utils';
import * as fs from 'fs';

const writeNullBit = (
    fd: number,
    masterNMapPageId: number,
    rowId: number,
    colIndex: number,
    numCols: number,
    isNull: boolean,
) => {
    let bitPosition = rowId * numCols + colIndex;

    let currentNMapPageId = masterNMapPageId;

    let nMapPage = readPage(fd, currentNMapPageId, 'writeNullBit');

    // loop until there's not a next page already existing
    // and the position of the is still not on this page
    while (nMapPage.nextPageId !== 0 && bitPosition >= BITS_PER_PAGE) {
        currentNMapPageId = nMapPage.nextPageId;
        nMapPage = readPage(fd, nMapPage.nextPageId, 'writeNullBit');
        bitPosition -= BITS_PER_PAGE;
    }

    // check if the bit is still not on the current page
    if (bitPosition >= BITS_PER_PAGE) {
        // if it's not and there are no more pages after
        // create a new page
        const newPageId = allocatePage(
            fd,
            PAGE_TYPES.MASTER_NULL_MAP,
            'writeNullBit',
        );

        // link the old last page to the new last page
        nMapPage.page.writeUInt32LE(newPageId, NEXT_PAGE_ID_POSITION);

        const written = fs.writeSync(
            fd,
            nMapPage.page,
            0,
            PAGE_SIZE,
            currentNMapPageId * PAGE_SIZE,
        );

        if (written !== PAGE_SIZE) {
            throw new StorageError(
                StorageErrorCode.SHORT_WRITE,
                'Short Write while linking Null Map page',
                {
                    context: {
                        pageId: currentNMapPageId,
                        expectedBytes: PAGE_SIZE,
                        actualBytes: written,
                        position: currentNMapPageId * PAGE_SIZE,
                    },
                },
            );
        }

        currentNMapPageId = newPageId;
        bitPosition -= BITS_PER_PAGE;
    }

    // finally update the value of the bit
    setBit(fd, currentNMapPageId, bitPosition, isNull);
};

export { writeNullBit };
