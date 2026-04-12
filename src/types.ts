interface BaseColumn {
    name: string;
}

export interface ForeignKeyColumn extends BaseColumn {
    type: 'foreign_key';
    foreignKey: {
        table: string;
        column: string;
    };
}

export interface BooleanColumn extends BaseColumn {
    type: 'boolean';
}

export interface IntegerColumn extends BaseColumn {
    type: 'integer';
}

export interface StringColumn extends BaseColumn {
    type: 'string';
}

export type NormalColumn = BooleanColumn | IntegerColumn | StringColumn;

export type Column = ForeignKeyColumn | NormalColumn;

type ColumnValue<T extends Column> =
    T extends ForeignKeyColumn ?
        number // usually FK = id
    : T extends { type: 'boolean' } ? boolean
    : T extends { type: 'integer' } ? number
    : T extends { type: 'string' } ? string
    : never;

export type RowObject<T extends readonly Column[]> = {
    [K in T[number] as K['name']]: ColumnValue<K>;
};

interface BaseResolvedColumn {
    name: string;
    columnDataId: number;
}

export interface ResolvedNormalColumn extends BaseResolvedColumn {
    type: 'boolean' | 'integer' | 'string';
}

export interface ResolvedFKColumn extends BaseResolvedColumn {
    type: 'foreign_key';
    foreignKey: {
        table: string;
        column: string;
        refPageId: number;
    };
}

export type ResolvedColumn = ResolvedNormalColumn | ResolvedFKColumn;

export interface Table {
    name: string;
    masterNMapPageId: number;
    nextRowId: number;
    slotMapId: number;
    colDefsPageId: number;
    catalogPageId: number;
    catalogSlotOffset: number;
    numCols: number;
}

export interface DatabaseContext {
    fd: number;
    tableCache: Map<string, Table>;
    columnCache: Map<string, Map<string, ResolvedColumn>>; // string - table name; Map<string, ResolvedColumn> - name -> column associaton
}

export const isForeignKey = (col: Column): col is ForeignKeyColumn =>
    col.type === 'foreign_key';

export interface pageType {
    nextPageId: number;
    nextOffset: number;
    recordCount: number;
    pageType: number;
    page: Buffer;
}
