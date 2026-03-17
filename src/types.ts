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

export interface NormalColumn extends BaseColumn {
    type: 'boolean' | 'integer' | 'string';
}

export type Column = ForeignKeyColumn | NormalColumn;

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
}

export interface DatabaseContext {
    fd: number;
    tableCache: Map<string, Table>;
    columnCache: Map<string, Map<string, ResolvedColumn>>; // string - table name; Map<string, ResolvedColumn> - name -> column associaton
}
