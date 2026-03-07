import { describe, expect, it } from 'vitest';

import {
    StorageError,
    StorageErrorCode,
    ValidationError,
    ValidationErrorCode,
} from '../src/errors';

describe('StorageError', () => {
    it('stores name, code, message and optional metadata', () => {
        const cause = new Error('boom');
        const error = new StorageError(
            StorageErrorCode.SHORT_WRITE,
            'short write happened',
            {
                context: { pageId: 10 },
                cause,
            },
        );

        expect(error).toBeInstanceOf(Error);
        expect(error.name).toBe('StorageError');
        expect(error.code).toBe(StorageErrorCode.SHORT_WRITE);
        expect(error.message).toBe('short write happened');
        expect(error.context).toEqual({ pageId: 10 });
        expect(error.cause).toBe(cause);
    });
});

describe('ValidationError', () => {
    it('stores name, code and message', () => {
        const error = new ValidationError(
            ValidationErrorCode.BAD_INPUT,
            'invalid input',
        );

        expect(error).toBeInstanceOf(Error);
        expect(error.name).toBe('ValidationError');
        expect(error.code).toBe(ValidationErrorCode.BAD_INPUT);
        expect(error.message).toBe('invalid input');
        expect(error.context).toBeUndefined();
        expect(error.cause).toBeUndefined();
    });
});
