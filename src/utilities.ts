import { HEADER_SIZE, PAGE_SIZE } from './constants';
import {
    StorageError,
    StorageErrorCode,
    ValidationError,
    ValidationErrorCode,
} from './errors';
import { readPage } from './page';
import * as fs from 'fs';

const setBit = (fd: number, pageId: number, bit: number, value: boolean) => {
    const BITMAP_CAPACITY = (PAGE_SIZE - HEADER_SIZE) * 8;

    if (bit >= BITMAP_CAPACITY) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'Bit should be within the current page capacity',
            {
                context: { bit, max: BITMAP_CAPACITY - 1 },
            },
        );
    }

    const { page } = readPage(fd, pageId, 'setBit');

    const BITMAP_OFFSET = HEADER_SIZE;
    const targetByte = BITMAP_OFFSET + Math.floor(bit / 8);
    const targetBit = bit % 8;

    if (targetByte >= page.length) {
        throw new StorageError(
            StorageErrorCode.OUT_OF_BOUNDS,
            'Bitmap write out of bounds',
            {
                context: {
                    targetByte,
                    pageSize: page.length,
                },
            },
        );
    }

    let byte = page[targetByte]!;

    if (value) {
        byte |= 1 << targetBit;
    } else {
        byte &= ~(1 << targetBit);
    }

    page[targetByte] = byte;

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
};
