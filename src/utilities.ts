import { HEADER_SIZE, PAGE_SIZE } from './constants';
import {
    StorageError,
    StorageErrorCode,
    ValidationError,
    ValidationErrorCode,
} from './errors';
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

    const targetByte = Math.floor(bit / 8);
    const targetBit = bit % 8;
    const fileOffset = pageId * PAGE_SIZE + HEADER_SIZE + targetByte;

    const byteBuffer = Buffer.alloc(1);
    const read = fs.readSync(fd, byteBuffer, 0, 1, fileOffset); // read only the byte which contains the bit we need to change

    if (read < 0) {
        throw new StorageError(
            StorageErrorCode.SHORT_READ,
            'Short read while reading bitmap byte for requested bit',
            {
                context: {
                    pageId,
                    bit,
                    targetByte,
                    expectedBytes: 1,
                    actualBytes: read,
                    position: fileOffset,
                },
            },
        );
    }

    let byte = byteBuffer[0]!;

    if (value) {
        byte |= 1 << targetBit;
    } else {
        byte &= ~(1 << targetBit);
    }

    byteBuffer[0] = byte;

    const written = fs.writeSync(fd, byteBuffer, 0, 1, fileOffset);

    if (written !== 1) {
        throw new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'Short Write while writing bitmap byte',
            {
                context: {
                    pageId,
                    expectedBytes: 1,
                    actualBytes: written,
                    position: fileOffset,
                },
            },
        );
    }

};
