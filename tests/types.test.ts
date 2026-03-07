import { describe, expectTypeOf, it } from 'vitest';

import type {
    Column,
    ForeignKeyColumn,
    NormalColumn,
    Table,
} from '../src/types';

describe('types', () => {
    it('defines the expected normal column shape', () => {
        expectTypeOf<NormalColumn['isForeignKey']>().toEqualTypeOf<false>();
        expectTypeOf<NormalColumn['type']>().toEqualTypeOf<
            'boolean' | 'integer' | 'string'
        >();
    });

    it('defines the expected foreign key column shape', () => {
        expectTypeOf<ForeignKeyColumn['isForeignKey']>().toEqualTypeOf<true>();
        expectTypeOf<
            ForeignKeyColumn['foreignKey']['table']
        >().toEqualTypeOf<string>();
        expectTypeOf<
            ForeignKeyColumn['foreignKey']['column']
        >().toEqualTypeOf<string>();
    });

    it('unions columns and defines table metadata shape', () => {
        expectTypeOf<Column>().toMatchTypeOf<NormalColumn | ForeignKeyColumn>();

        const table: Table = {
            name: 'users',
            masterNMapPageId: 10,
            colDefsPageId: 11,
        };

        expectTypeOf(table).toMatchTypeOf<Table>();
    });
});
