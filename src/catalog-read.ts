import {
    COL_SLOT,
    COLUMN_SLOT_SIZE,
    DATA_TYPE_LOOKUP,
    DATA_TYPES,
    HEADER_SIZE,
    TABLE_SLOT,
    TABLE_SLOT_SIZE,
} from './constants';
import { ValidationError, ValidationErrorCode } from './errors';
import { readPage } from './page';
import { Column, DatabaseContext, ResolvedColumn, Table } from './types';

const getTable = (name: string, db: DatabaseContext): Table => {
    const cachedTable = db.tableCache.get(name);

    if (cachedTable) {
        return cachedTable;
    }

    const nameBuffer = Buffer.from(name, 'utf8');

    if (nameBuffer.length >= 12) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'The name should not exceed 12 bytes',
        );
    }

    const fd = db.fd;
    let parsedPage = readPage(fd, 1, 'getTable');
    let offset = HEADER_SIZE;
    let page = parsedPage.page;
    let nextPageId = parsedPage.nextPageId;
    let tableCount = parsedPage.recordCount;

    while (true) {
        for (let i = 0; i < tableCount; i++) {
            // early break condition as a empty name means no more entries
            if (page[offset + TABLE_SLOT.NAME] === 0) {
                break;
            }

            // check if the name of the table matches the name we're searching for
            if (
                nameBuffer.compare(
                    page,
                    offset + TABLE_SLOT.NAME,
                    offset + TABLE_SLOT.NAME + nameBuffer.length,
                ) !== 0 ||
                page[offset + TABLE_SLOT.NAME + nameBuffer.length] !== 0
            ) {
                offset += TABLE_SLOT_SIZE;
                continue;
            }

            const masterNMapPageId = page.readUInt32LE(
                offset + TABLE_SLOT.MASTER_NMAP_PAGE_ID,
            );

            const nextRowId = page.readUInt32LE(
                offset + TABLE_SLOT.NEXT_ROW_ID,
            );

            const slotMapId = page.readUInt32LE(offset + TABLE_SLOT.SLOT_MAP);

            const colDefsPageId = page.readUInt32LE(
                offset + TABLE_SLOT.COL_DEFS,
            );

            // update the cache
            db.tableCache.set(name, {
                name,
                masterNMapPageId,
                nextRowId,
                slotMapId,
                colDefsPageId,
            });

            return {
                name,
                masterNMapPageId,
                nextRowId,
                slotMapId,
                colDefsPageId,
            };
        }

        // if nextPageId = 0 that means that there are no more table definition pages
        // and that the table with the specified name does not exist
        if (nextPageId === 0) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'There exists no table with the name ' + name,
            );
        }

        // go to the next page
        parsedPage = readPage(fd, nextPageId, 'getTable');
        offset = HEADER_SIZE;
        page = parsedPage.page;
        nextPageId = parsedPage.nextPageId;
        tableCount = parsedPage.recordCount;
    }
};

const getColumn = (
    name: string,
    table: Table,
    db: DatabaseContext,
): ResolvedColumn => {
    const cachedColumn = db.columnCache.get(table.name)?.get(name);

    if (cachedColumn) {
        return cachedColumn;
    }

    const nameBuffer = Buffer.from(name, 'utf8');

    if (nameBuffer.length >= 12) {
        throw new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'The name should not exceed 12 bytes',
        );
    }

    const fd = db.fd;
    let parsedPage = readPage(fd, table.colDefsPageId, 'getColumn');
    let offset = HEADER_SIZE;
    let page = parsedPage.page;
    let nextPageId = parsedPage.nextPageId;
    let columnCount = parsedPage.recordCount;

    while (true) {
        for (let i = 0; i < columnCount; i++) {
            // early break condition as a empty name means no more entries
            if (page[offset + COL_SLOT.NAME] === 0) {
                break;
            }

            // check if the name of the table matches the name we're searching for
            if (
                nameBuffer.compare(
                    page,
                    offset + COL_SLOT.NAME,
                    offset + COL_SLOT.NAME + nameBuffer.length,
                ) !== 0 ||
                page[offset + COL_SLOT.NAME + nameBuffer.length] !== 0
            ) {
                offset += COLUMN_SLOT_SIZE;
                continue;
            }

            const dataType = page.readUInt8(offset + COL_SLOT.TYPE);

            let fkTableName = '',
                fkColumnName = '',
                fkRefPageId = 0;
            if (dataType === DATA_TYPES.FOREIGN_KEY) {
                let endOffset, nullPos;

                // find the exact foreign key reference names (remove any trailing null bytes)
                nullPos = page.indexOf(0, offset + COL_SLOT.FK_TABLE);
                endOffset =
                    nullPos === -1 || nullPos > offset + COL_SLOT.FK_COL ?
                        offset + COL_SLOT.FK_COL
                    :   nullPos;

                fkTableName = page.toString(
                    'utf8',
                    offset + COL_SLOT.FK_TABLE,
                    endOffset,
                );

                nullPos = page.indexOf(0, offset + COL_SLOT.FK_COL);

                endOffset =
                    (
                        nullPos === -1 ||
                        nullPos > offset + COL_SLOT.FK_REF_PAGE_ID
                    ) ?
                        offset + COL_SLOT.FK_REF_PAGE_ID
                    :   nullPos;

                fkColumnName = page.toString(
                    'utf8',
                    offset + COL_SLOT.FK_COL,
                    endOffset,
                );

                fkRefPageId = page.readUInt32LE(
                    offset + COL_SLOT.FK_REF_PAGE_ID,
                );
            }

            const columnDataId = page.readUInt32LE(
                offset +
                    (dataType === DATA_TYPES.FOREIGN_KEY ?
                        COL_SLOT.FK_DATA_PAGE_ID
                    :   COL_SLOT.DATA_PAGE_ID),
            );

            const type = DATA_TYPE_LOOKUP[
                dataType as keyof typeof DATA_TYPE_LOOKUP
            ] as Column['type'];

            if (!type) {
                throw new ValidationError(
                    ValidationErrorCode.BAD_INPUT,
                    'Bad data type',
                );
            }

            // update the cache (tableTBU - table to be updated)
            if (type === 'foreign_key') {
                let tableTBU = db.columnCache.get(table.name);

                if (!tableTBU) {
                    tableTBU = new Map<string, ResolvedColumn>();
                    db.columnCache.set(table.name, tableTBU);
                }

                tableTBU.set(name, {
                    name,
                    type,
                    columnDataId,
                    foreignKey: {
                        table: fkTableName,
                        column: fkColumnName,
                        refPageId: fkRefPageId,
                    },
                });

                return {
                    name,
                    type,
                    columnDataId,
                    foreignKey: {
                        table: fkTableName,
                        column: fkColumnName,
                        refPageId: fkRefPageId,
                    },
                };
            } else {
                let tableTBU = db.columnCache.get(table.name);

                if (!tableTBU) {
                    tableTBU = new Map<string, ResolvedColumn>();
                    db.columnCache.set(table.name, tableTBU);
                }

                tableTBU.set(name, {
                    name,
                    type,
                    columnDataId,
                });

                return {
                    name,
                    type,
                    columnDataId,
                };
            }
        }

        // if nextPageId = 0 that means that there are no more column definition pages
        // and that the column with the specified name does not exist
        if (nextPageId === 0) {
            throw new ValidationError(
                ValidationErrorCode.BAD_INPUT,
                'There exists no column with the name ' + name,
            );
        }

        // go to the next page
        parsedPage = readPage(fd, nextPageId, 'getColumn');
        offset = HEADER_SIZE;
        page = parsedPage.page;
        nextPageId = parsedPage.nextPageId;
        columnCount = parsedPage.recordCount;
    }
};

export { getTable, getColumn };
