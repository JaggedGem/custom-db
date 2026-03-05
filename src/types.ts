interface BaseColumn {
    name: string;
}

export interface ForeignKeyColumn extends BaseColumn {
    isForeignKey: true;
    foreignKey: {
        table: string;
        column: string;
    };
}

export interface NormalColumn extends BaseColumn {
    type: 'boolean' | 'integer' | 'string';
    isForeignKey: false;
    foreignKey?: never;
}

export type Column = ForeignKeyColumn | NormalColumn;
