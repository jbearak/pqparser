// pqparser/node: open a Parquet file on disk.

import * as fs from "node:fs";
import { ParquetReader, type ParquetOptions } from "./parquet";

/**
 * Open a Parquet file by path, or by a file descriptor the caller owns
 * and closes. Opening reads only the footer; each read then reads,
 * through `fs.readSync`, only the parts of the file it needs.
 */
export function openParquetFile(file: string | number, options: ParquetOptions = {}): ParquetReader {
  const owned = typeof file === "string";
  const fd = owned ? fs.openSync(file, "r") : file;
  try {
    const length = fs.fstatSync(fd).size;
    const read = (into: Uint8Array, offset: number) => {
      let done = 0;
      while (done < into.length) {
        // Node rejects reads of 2 GiB or more, so take at most 1 GiB a time.
        const n = fs.readSync(fd, into, done, Math.min(into.length - done, 1 << 30), offset + done);
        if (n === 0) throw new RangeError("unexpected end of file");
        done += n;
      }
    };
    return new ParquetReader(read, length, options, owned ? () => fs.closeSync(fd) : undefined);
  } catch (e) {
    if (owned) fs.closeSync(fd);
    throw e;
  }
}

export type { ParquetOptions, ParquetReader };
