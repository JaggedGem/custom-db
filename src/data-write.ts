import { getColumn, getTable } from './catalog-read';
import {
    COL_SLOT,
    COLUMN_SLOT_SIZE,
    FREE_SPACE_POINTER_POSITION,
    HEADER_SIZE,
    NEXT_PAGE_ID_POSITION,
    NEXT_SLOT_OFFSET_POSITION,
    PAGE_SIZE,
    PAGE_TYPES,
    RECORD_COUNT_POSITION,
    SLOT_DESCRIPTOR_SIZE,
} from './constants';
import {
    StorageError,
    StorageErrorCode,
    ValidationError,
    ValidationErrorCode,
} from './errors';
import { allocatePage, getLatestPage, readPage } from './page';
import { Column, DatabaseContext, ResolvedColumn, RowObject } from './types';
import * as fs from 'fs';
import { setBit } from './utils';
import { createBitmapPage, createSlottedPage } from './data-pages';
import { writeSlotMapEntry } from './slot-map';
import { persistNextRowId } from './catalog-write';
import { writeNullBit } from './null-map';

const insertRow = <T extends readonly Column[]>(
    tableName: string,
    data: RowObject<T>,
    db: DatabaseContext,
) => {
    const table = getTable(tableName, db);

    const colDefs: Record<string, ResolvedColumn> = {};

    const colIndexes: Record<string, number> = {};
    let i = 0;

    let currentPageId = table.colDefsPageId;

    // get all of the columns and store them in the colDefs
    while (currentPageId !== 0) {
        const page = readPage(db.fd, currentPageId, 'insertRow');

        for (let j = 0; j < page.recordCount; j++) {
            const offset = HEADER_SIZE + j * COLUMN_SLOT_SIZE;
            const nameStart = offset + COL_SLOT.NAME;
            const nameFieldEnd = offset + COL_SLOT.TYPE;

            // find the exact column name (remove any trailing null bytes)
            const nullPos = page.page.indexOf(0, nameStart);
            const endOffset =
                nullPos === -1 || nullPos > nameFieldEnd ?
                    nameFieldEnd
                :   nullPos;

            const name = page.page.toString('utf8', nameStart, endOffset);

            colDefs[name] = getColumn(name, table, db);

            colIndexes[name] = i;

            i++;
        }

        currentPageId = page.nextPageId;
    }

    const numCols = table.numCols;

    if (numCols !== Object.keys(data).length) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'Number of columns to be inserted should match number of columns in the table', // todo: handle allowed nulls
            {
                context: {
                    expectedLength: numCols,
                    actualLength: Object.keys(data).length,
                },
            },
        );
    }

    const slotIndex = table.nextRowId;

    for (const [ colName, value ] of Object.entries(data)) {
        const currentCol = colDefs[colName];

        if (!currentCol) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'Column ' + colName + ' does not exist on table ' + tableName,
            );
        }

        const typeMap: Record<string, string> = {
            integer: 'number',
            string: 'string',
            boolean: 'boolean',
            foreign_key: 'number',
        };

        if (typeof value !== typeMap[currentCol.type]) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'Column ' + colName + ' does not match the type expected',
                {
                    context: {
                        expectedType: currentCol.type,
                        actualType: typeof value,
                    },
                },
            );
        }

        const expectedPageType =
            currentCol.type === 'string' ? PAGE_TYPES.DATA_SLOTTED
            : (
                currentCol.type === 'integer' ||
                currentCol.type === 'foreign_key'
            ) ?
                PAGE_TYPES.DATA_FIXED
            :   PAGE_TYPES.DATA_BITMAP;

        let targetPageId = getLatestPage(
            db.fd,
            currentCol.columnDataId,
            expectedPageType,
        );

        let page = readPage(db.fd, targetPageId, 'insertRow');

        switch (currentCol.type) {
            case 'string': {
                // type narrow to string
                if (typeof value !== 'string') {
                    throw new ValidationError(
                        ValidationErrorCode.BAD_INPUT,
                        'Column ' +
                            colName +
                            ' does not match the type expected',
                        {
                            context: {
                                expectedType: currentCol.type,
                                actualType: typeof value,
                            },
                        },
                    );
                }

                const valueByteLength = Buffer.byteLength(value, 'utf8');
                let dataFreeSpace = page.page.readUInt16LE(
                    FREE_SPACE_POINTER_POSITION,
                );

                // handle page overflow
                if (
                    dataFreeSpace - valueByteLength <
                    HEADER_SIZE + (page.recordCount + 1) * SLOT_DESCRIPTOR_SIZE
                ) {
                    const newPageId = createSlottedPage(db.fd);

                    // link the current page to the new one
                    page.page.writeUInt32LE(newPageId, NEXT_PAGE_ID_POSITION);

                    const written = fs.writeSync(
                        db.fd,
                        page.page,
                        0,
                        PAGE_SIZE,
                        targetPageId * PAGE_SIZE,
                    );

                    if (written !== PAGE_SIZE) {
                        throw new StorageError(
                            StorageErrorCode.SHORT_WRITE,
                            'Short Write while linking slotted data page',
                            {
                                context: {
                                    pageId: targetPageId,
                                    expectedBytes: PAGE_SIZE,
                                    actualBytes: written,
                                    position: targetPageId * PAGE_SIZE,
                                },
                            },
                        );
                    }

                    targetPageId = newPageId;

                    page = readPage(db.fd, targetPageId, 'insertRow');

                    dataFreeSpace = page.page.readUInt16LE(
                        FREE_SPACE_POINTER_POSITION,
                    );
                }

                // actual data writing logic
                page.page.fill(
                    0,
                    dataFreeSpace - valueByteLength,
                    dataFreeSpace,
                );

                page.page.write(value, dataFreeSpace - valueByteLength, 'utf8');

                // add the slot descriptor for the string
                const dataOffset = dataFreeSpace - valueByteLength;
                page.page.writeUInt16LE(
                    dataOffset,
                    HEADER_SIZE + page.recordCount * SLOT_DESCRIPTOR_SIZE,
                ); // write the start of the data
                page.page.writeUInt16LE(
                    valueByteLength,
                    HEADER_SIZE + page.recordCount * SLOT_DESCRIPTOR_SIZE + 2,
                ); // write the length of the data

                // update the data free space pointer
                page.page.writeUInt16LE(
                    dataOffset,
                    FREE_SPACE_POINTER_POSITION,
                );

                // update record count
                page.page.writeUInt16LE(
                    page.recordCount + 1,
                    RECORD_COUNT_POSITION,
                );

                // write to disk
                const written = fs.writeSync(
                    db.fd,
                    page.page,
                    0,
                    PAGE_SIZE,
                    targetPageId * PAGE_SIZE,
                );

                if (written !== PAGE_SIZE) {
                    throw new StorageError(
                        StorageErrorCode.SHORT_WRITE,
                        'Short Write while writing page header',
                        {
                            context: {
                                pageId: targetPageId,
                                expectedBytes: PAGE_SIZE,
                                actualBytes: written,
                                position: targetPageId * PAGE_SIZE,
                            },
                        },
                    );
                }

                break;
            }

            case 'integer': {
                // type narrow to number
                if (typeof value !== 'number') {
                    throw new ValidationError(
                        ValidationErrorCode.BAD_INPUT,
                        'Column ' +
                            colName +
                            ' does not match the type expected',
                        {
                            context: {
                                expectedType: currentCol.type,
                                actualType: typeof value,
                            },
                        },
                    );
                }

                // handle page overflow
                if (page.nextOffset + 4 > PAGE_SIZE) {
                    // 4 - size of a signed 32 bit integer in bytes
                    const newPageId = allocatePage(
                        db.fd,
                        expectedPageType,
                        'insertRow',
                    );

                    // link the current page to the new one
                    page.page.writeUInt32LE(newPageId, NEXT_PAGE_ID_POSITION);

                    const written = fs.writeSync(
                        db.fd,
                        page.page,
                        0,
                        PAGE_SIZE,
                        targetPageId * PAGE_SIZE,
                    );

                    if (written !== PAGE_SIZE) {
                        throw new StorageError(
                            StorageErrorCode.SHORT_WRITE,
                            'Short Write while linking slotted data page',
                            {
                                context: {
                                    pageId: targetPageId,
                                    expectedBytes: PAGE_SIZE,
                                    actualBytes: written,
                                    position: targetPageId * PAGE_SIZE,
                                },
                            },
                        );
                    }

                    targetPageId = newPageId;

                    page = readPage(db.fd, targetPageId, 'insertRow');
                }

                // actual data writing logic
                page.page.writeInt32LE(value, page.nextOffset);

                // update next offset value
                page.page.writeUInt16LE(
                    page.nextOffset + 4,
                    NEXT_SLOT_OFFSET_POSITION,
                );

                // update record count
                page.page.writeUInt16LE(
                    page.recordCount + 1,
                    RECORD_COUNT_POSITION,
                );

                // write to disk
                const written = fs.writeSync(
                    db.fd,
                    page.page,
                    0,
                    PAGE_SIZE,
                    targetPageId * PAGE_SIZE,
                );

                if (written !== PAGE_SIZE) {
                    throw new StorageError(
                        StorageErrorCode.SHORT_WRITE,
                        'Short Write while writing page header',
                        {
                            context: {
                                pageId: targetPageId,
                                expectedBytes: PAGE_SIZE,
                                actualBytes: written,
                                position: targetPageId * PAGE_SIZE,
                            },
                        },
                    );
                }

                break;
            }

            case 'boolean': {
                // type narrow to boolean
                if (typeof value !== 'boolean') {
                    throw new ValidationError(
                        ValidationErrorCode.BAD_INPUT,
                        'Column ' +
                            colName +
                            ' does not match the type expected',
                        {
                            context: {
                                expectedType: currentCol.type,
                                actualType: typeof value,
                            },
                        },
                    );
                }

                if (page.recordCount + 1 > (PAGE_SIZE - HEADER_SIZE) * 8) {
                    const newPageId = createBitmapPage(db.fd);

                    // link the current page to the new one
                    page.page.writeUInt32LE(newPageId, NEXT_PAGE_ID_POSITION);

                    const written = fs.writeSync(
                        db.fd,
                        page.page,
                        0,
                        PAGE_SIZE,
                        targetPageId * PAGE_SIZE,
                    );

                    if (written !== PAGE_SIZE) {
                        throw new StorageError(
                            StorageErrorCode.SHORT_WRITE,
                            'Short Write while linking bitmap data page',
                            {
                                context: {
                                    pageId: targetPageId,
                                    expectedBytes: PAGE_SIZE,
                                    actualBytes: written,
                                    position: targetPageId * PAGE_SIZE,
                                },
                            },
                        );
                    }

                    targetPageId = newPageId;

                    page = readPage(db.fd, targetPageId, 'insertRow');
                }

                setBit(db.fd, targetPageId, page.recordCount, value);

                page = readPage(db.fd, targetPageId, 'insertRow');

                // update record count
                page.page.writeUInt16LE(
                    page.recordCount + 1,
                    RECORD_COUNT_POSITION,
                );

                // write to disk
                const written = fs.writeSync(
                    db.fd,
                    page.page,
                    0,
                    PAGE_SIZE,
                    targetPageId * PAGE_SIZE,
                );

                if (written !== PAGE_SIZE) {
                    throw new StorageError(
                        StorageErrorCode.SHORT_WRITE,
                        'Short Write while writing data to page',
                        {
                            context: {
                                pageId: targetPageId,
                                expectedBytes: PAGE_SIZE,
                                actualBytes: written,
                                position: targetPageId * PAGE_SIZE,
                            },
                        },
                    );
                }

                break;
            }

            case 'foreign_key': {
                // type narrow to number
                if (typeof value !== 'number') {
                    throw new ValidationError(
                        ValidationErrorCode.BAD_INPUT,
                        'Column ' +
                            colName +
                            ' does not match the type expected',
                        {
                            context: {
                                expectedType: currentCol.type,
                                actualType: typeof value,
                            },
                        },
                    );
                }

                // handle page overflow
                if (page.nextOffset + 4 > PAGE_SIZE) {
                    // 4 - size of a unsigned 32 bit integer in bytes
                    const newPageId = allocatePage(
                        db.fd,
                        expectedPageType,
                        'insertRow',
                    );

                    // link the current page to the new one
                    page.page.writeUInt32LE(newPageId, NEXT_PAGE_ID_POSITION);

                    const written = fs.writeSync(
                        db.fd,
                        page.page,
                        0,
                        PAGE_SIZE,
                        targetPageId * PAGE_SIZE,
                    );

                    if (written !== PAGE_SIZE) {
                        throw new StorageError(
                            StorageErrorCode.SHORT_WRITE,
                            'Short Write while linking fixed data page',
                            {
                                context: {
                                    pageId: targetPageId,
                                    expectedBytes: PAGE_SIZE,
                                    actualBytes: written,
                                    position: targetPageId * PAGE_SIZE,
                                },
                            },
                        );
                    }

                    targetPageId = newPageId;

                    page = readPage(db.fd, targetPageId, 'insertRow');
                }

                // actual data writing logic
                page.page.writeUInt32LE(value, page.nextOffset);

                // update next offset value
                page.page.writeUInt16LE(
                    page.nextOffset + 4,
                    NEXT_SLOT_OFFSET_POSITION,
                );

                // update record count
                page.page.writeUInt16LE(
                    page.recordCount + 1,
                    RECORD_COUNT_POSITION,
                );

                // write to disk
                const written = fs.writeSync(
                    db.fd,
                    page.page,
                    0,
                    PAGE_SIZE,
                    targetPageId * PAGE_SIZE,
                );

                if (written !== PAGE_SIZE) {
                    throw new StorageError(
                        StorageErrorCode.SHORT_WRITE,
                        'Short Write while writing data to page',
                        {
                            context: {
                                pageId: targetPageId,
                                expectedBytes: PAGE_SIZE,
                                actualBytes: written,
                                position: targetPageId * PAGE_SIZE,
                            },
                        },
                    );
                }

                break;
            }
        }

        // write the nullmap to show that the newly
        // introduced value is not null
        const colIndex = colIndexes[colName];
        if (colIndex === undefined) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'Column index not found for column: ' + colName,
            );
        }

        writeNullBit(db.fd, table.masterNMapPageId, table.nextRowId, colIndex, numCols, false);
    }

    fs.fsyncSync(db.fd);

    writeSlotMapEntry(db.fd, table.slotMapId, table.nextRowId, slotIndex);

    // update next row id in the table definition and the cache
    table.nextRowId += 1;
    persistNextRowId(db, table);
    db.tableCache.set(tableName, table);

    return slotIndex;
};

export { insertRow };
