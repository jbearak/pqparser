// Errors carry a `code`, as @jbearak/dta-parser's do, so callers that
// tell its errors apart tell these apart the same way.

/**
 * What went wrong:
 * - `DTA_CORRUPT`: the file is damaged or not a Parquet file;
 * - `DTA_TOO_LARGE`: a read does not fit in memory here;
 * - `DTA_IO`: reading the file failed (the underlying error is the `cause`);
 * - `DTA_INVALID`: an argument is wrong, or the column cannot be read;
 * - `DTA_OUT_OF_RANGE`: a variable or rows the file does not have (a `RangeError`);
 * - `DTA_CLOSED`: the reader was closed;
 * - `DTA_INTERNAL`: a bug in pqparser.
 */
export type ErrorCode = "DTA_CORRUPT" | "DTA_TOO_LARGE" | "DTA_IO" | "DTA_INVALID" | "DTA_OUT_OF_RANGE" | "DTA_CLOSED" | "DTA_INTERNAL";

/** An error thrown by the package: an `Error` (or `TypeError`, `RangeError`) with a `code`. */
export interface DtaError extends Error {
  code: ErrorCode;
}

type ErrorClass = ErrorConstructor | TypeErrorConstructor | RangeErrorConstructor;

/** An error of class `kind` with `code`. */
export function dtaError(code: ErrorCode, message: string, cause?: unknown, kind: ErrorClass = Error): DtaError {
  const e = new kind(message, cause === undefined ? undefined : { cause }) as DtaError;
  e.code = code;
  return e;
}
