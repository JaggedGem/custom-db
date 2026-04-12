export enum StorageErrorCode {
    SHORT_WRITE = 'SHORT_WRITE',
    SHORT_READ = 'SHORT_READ',
    IO_ERROR = 'IO_ERROR',
    OUT_OF_BOUNDS = 'OUT_OF_BOUNDS'
}

export class StorageError extends Error {
    readonly code: StorageErrorCode;
    readonly context?: Record<string, unknown>;
    readonly cause?: unknown;

    constructor(
        code: StorageErrorCode,
        message: string,
        options?: {
            context?: Record<string, unknown>;
            cause?: unknown;
        },
    ) {
        super(message);
        this.name = 'StorageError';
        this.code = code;

        if (options?.context !== undefined) {
            this.context = options.context;
        }

        if (options?.cause !== undefined) {
            this.cause = options.cause;
        }
    }
}

export enum ValidationErrorCode {
    BAD_INPUT = 'BAD_INPUT',
}
export class ValidationError extends Error {
    readonly code: ValidationErrorCode;
    readonly context?: Record<string, unknown>;
    readonly cause?: unknown;

    constructor(
        code: ValidationErrorCode,
        message: string,
        options?: {
            context?: Record<string, unknown>;
            cause?: unknown;
        },
    ) {
        super(message);
        this.name = 'ValidationError';
        this.code = code;

        if (options?.context !== undefined) {
            this.context = options.context;
        }

        if (options?.cause !== undefined) {
            this.cause = options.cause;
        }
    }
}
