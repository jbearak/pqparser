# pqparser

A read-only Parquet reader for Node, written in TypeScript over
[arrow-rs](https://github.com/apache/arrow-rs) (the `parquet` crate 60.1)
compiled to WebAssembly.

It replaces the Parquet half of the npm package `@jbearak/dta-parser`
1.1.0 for table-viewer: it
exports the same names, with the same shapes, for everything table-viewer
calls, so table-viewer switches by changing its imports. The package is
private and is not published.

## Use

```js
const { openParquet } = require("pqparser");         // a file held in memory
const { openParquetFile } = require("pqparser/node"); // a path, or a descriptor the caller owns

const r = openParquetFile("survey.parquet", { nested: "error", int64: "auto", pageCacheBytes: 32 << 20 });
r.info;        // rows, row groups, codecs, writer, key-value entries, warnings
r.variables;   // name, type, format, labels, Parquet types, column kind, chunks (sizes, codec, encodings, statistics)
r.valueLabels("agelab");
r.readColumns([0, "age"], { start: 0, count: 2048 });
r.readRowsAt([5, 900, 5], [0, 1]);
r.close();
```

Opening reads only the footer (and the offset indexes, when there are
any). Each read reads, through `fs.readSync` for a file, only the pages
it needs, so a large file is never read whole.

### Switching table-viewer

In `src/data-source/parquet-source.ts`, import `openParquet` and the
types `ColumnChunk`, `DtaError`, `ParquetOptions`, `ParquetReader`,
`ParquetVariable`, `Statistic`, `TableCell`, `TableColumn` and
`ValueLabels` from `pqparser`; in `file-parquet-source.ts`, `ParquetReader`
from `pqparser` and `openParquetFile` from `pqparser/node`. `formatValue`
(Stata display formats) stays an import from `@jbearak/dta-parser`.

`MISSING`, `isMissing` and `missingValue` are exported too. The 27
missing values are made once per JavaScript realm, under the
`Symbol.for` key that `@jbearak/dta-parser` 2 uses, so a value label set
pqparser reads is keyed by the same objects dta-parser 2 hands out.
dta-parser 1.1.0 makes its own objects, so with 1.1.0 they are equal by
name (`.missing`), not by identity.

## What it reads

What 1.1.0 reads, the way 1.1.0 reads it, for everything table-viewer
reaches: the variables' types (`byte` to `double`, `int64`, `uint8` to
`uint64`, `bool`, `date`, `timestamp`, `time`, `decimal`, `string`,
`binary`, `categorical`), Parquet's physical and logical types, the
column kinds `readColumns` returns, `int64: "auto"` decided from the
footer's statistics, units and time zones (from the `dta` profile or
`ARROW:schema`), and each row group's chunk (sizes, codec, encodings,
dictionary page, null count, minimum and maximum). From the `dta` profile
(see dta-parser's `docs/parquet.md`): the data label, notes, sort order,
variable labels, display formats, value label sets, categoricals with
their levels, and DOUBLE columns written wider because they hold `.a` to
`.z`. `.a` to `.z` are read from tagged NaNs into the missing codes 2 to
27. Codecs: none, snappy, gzip, zstd, brotli, LZ4_RAW and LZ4 (in
Hadoop's framing, or without it, as some writers made it): all but LZO,
which arrow-rs does not read. DECIMAL of up to 15 digits reads as
numbers; of more, as text with every digit, as 1.1.0 read it.

Beyond 1.1.0 (see "Differences"): FLOAT16 as numbers, GEOMETRY and
GEOGRAPHY as bytes (well-known binary, as pyarrow reads them), and a
logical type newer than arrow-rs as its physical type.

## Differences from 1.1.0

Dropped on purpose:

- **`nested: "skip"`**: refused. Nested columns are in `variables` with
  role `"nested"`, and reading them is an error naming them, as before.
- **`pageCacheBytes`**: accepted and ignored. arrow-rs reads only the
  pages a read needs (with an offset index; without one, it reads the
  page headers before them too) and keeps none, so neighbouring windows
  decompress a page again. `readRowsAt` reads every row asked for in one
  pass, so each page it needs is decompressed once, and skips the rows
  between them rather than decoding them (arrow-rs's `Selectors` policy).
- Not here, since table-viewer does not use them: `openParquetAsync` and
  `AsyncParquetReader`, the writers, `characteristics()`, `notes()`,
  `keyValue()`, `readColumns`' `strings: "dictionary"` and
  `maxStringBytes`, `toDate`, `dtaKey`, `formatValue`, and everything for
  `.dta` files.

Read here and not by 1.1.0:

- FLOAT16 (a `number` column of type `float16`, without statistics),
  GEOMETRY and GEOGRAPHY (bytes), LZ4 in Hadoop's framing, ALP-encoded
  floats, and data pages v2 with no bytes or with repetition levels on a
  flat column.
- Text that is not UTF-8 in a STRING, ENUM or JSON column: each value
  that is not reads with its bad bytes replaced by U+FFFD (1.1.0 refused
  the read). arrow-rs checks STRING columns: the first read that meets
  one reads it again as bytes, and every later read of the file reads
  text so. It checks neither ENUM (read as bytes) nor JSON columns, whose
  values are mended as they are copied out.

From arrow-rs:

- `ColumnChunk.encodings` lists each encoding once, in the order of
  Parquet's enum (arrow-rs keeps them as a set); 1.1.0 kept the footer's
  order. They agree in every fixture; table-viewer only asks whether one
  is present.
- arrow-rs checks the footer more strictly than 1.1.0 did. Where it
  refuses a footer that other readers take, the module mends the footer
  and reads it again:
  - Parquet2.jl writes a DECIMAL's precision and scale only in its
    logical type: they are added.
  - An annotation that does not fit its physical type (UUID on an INT32
    or a FIXED_LEN_BYTE_ARRAY(4), TIMESTAMP on a FIXED_LEN_BYTE_ARRAY(12))
    is dropped, and the column read as its physical type, as
    parquet-testing's README asks of readers; `info.warnings` names the
    column. Only that element's: of the elements with the name arrow-rs
    gives, the first whose annotation, dropped, changes arrow-rs's answer.
    1.1.0 left such a column out.
  - A logical type newer than arrow-rs (on an integer, arrow-rs would
    refuse to read any column) is dropped, and the column read as its
    physical type, as Parquet asks of readers.
  - A column chunk's field of the wrong Thrift type (Dremio writes a list
    as `bloom_filter_length`) is left out, and a dictionary page offset
    of 0 is read as none (the dictionary page is then the chunk's first
    page).
  - A file row count that is not the sum of the row groups' (parquet-rs
    0.3 wrote 0) is taken from the row groups.
  - An offset index that does not read is left out, and so are offset
    indexes that span more than 64 MiB of the file (arrow-rs reads them
    in one piece, gaps included, when the file opens). Without them a
    read reads the page headers before the pages it needs.
- arrow-rs turns INT96 into nanoseconds since 1970 with wrapping
  arithmetic, so a timestamp before 1677 or after 2262 (Spark writes
  9999-12-31 so) comes out wrong. pqparser reads INT96 columns again in
  seconds and makes such a value an error naming its row, as 1.1.0 did.

Neither reads `nation.dict-malformed` from parquet-testing, whose
dictionary pages run past their column chunks (pyarrow reads it).

Damaged files give an `Error` with a `code` (`DTA_CORRUPT` for a damaged
file, `DTA_IO` when the file system fails), never a crash: a panic in
the module stops only that reader's instance of it, and later calls on
that reader say so.

## Build and test

Needs Rust 1.88 or later with the `wasm32-unknown-unknown` target, a
clang and llvm-ar that target wasm32 (for zstd's C code: Homebrew's llvm
on macOS, `clang` and `llvm` packages on Linux), and Node 24 or later.

    rustup target add wasm32-unknown-unknown
    sh scripts/build-wasm.sh     # build/pqparser.wasm
    npm ci
    npm run build                # dist/, the module embedded brotli-compressed
    npm test

The tests (node:test, `test/`):

- `differential.test.mjs`: every fixture opened with pqparser and with
  1.1.0 (a devDependency at exactly 1.1.0): `info`, `variables` with
  their chunks, value labels, `readColumns` of every column whole and in
  windows across row groups, a variable asked for twice, and `readRowsAt`
  of scattered rows, value for value and missing code for missing code;
  with `int64` `"auto"`, `"bigint"` and `"number"`. The differences above
  are asserted as such.
- `pyarrow.test.mjs`: every fixture against what pyarrow reads from it
  (the JSON beside the fixtures), by the rules of dta-parser's
  `crates/parquet/tests/compat.rs`; for the legacy files, also the `dta`
  profile's labels, formats and value label sets.
- `replay.test.mjs`: table-viewer's own calls, in its order, from opening
  by file descriptor (and from bytes) to closing, against both readers.
- `api.test.mjs`: options, error codes and classes, brotli, missing values.
- `damaged.test.mjs`: truncated files, garbage, cut footers, and a
  thousand files with bytes changed.
- `corpus.test.mjs`: Apache's parquet-testing corpus (70 files, see
  `test/fixtures/parquet-testing/README.md`) against what pyarrow reads
  from it (`scripts/expected.py`), with the exceptions listed in the test.

The fixtures are dta-parser's `compat/parquet` files (commit 0ab136d;
see `test/fixtures/README.md`) and Apache's parquet-testing files
(`test/fixtures/parquet-testing/README.md`).

`node scripts/bench.mjs FILE.parquet` times a whole-file `readColumns`
with pqparser and 1.1.0. `python3 scripts/notices.py` writes
`THIRD-PARTY-NOTICES` after the crates change.

## Size

The module is 4.0 MB (opt-level `"s"`), 923 KB brotli-compressed, which
is how `dist/wasm-data.js` holds it (1.23 MB as base64). The brotli codec
is 1.05 MB of that (420 KB compressed): arrow-rs's codec can also
compress, so brotli's encoder comes along. Without brotli, at opt-level
`"z"` the module was 2.5 MB but reads of numbers took twice as long; at
3, 4.1 MB and no faster than `"s"`.

## Speed

Whole-file `readColumns`, Node 26 on an Apple M-series laptop
(`scripts/bench.mjs`), pqparser against 1.1.0:

| file | first call (module load included) | warm, median |
|---|---|---|
| `hyparquet_delta` (the largest fixture, 600 rows, 76 KB) | 22 ms against 16 ms | 0.95 ms against 0.57 ms |
| 1,000,000 rows, 5 columns, 19 MB, written by 1.1.0 | 100 ms against 136 ms | 69 ms against 98 ms |

Those are 0.0.1's. 0.0.2's larger module (brotli) adds about 8 ms to
the first call, and 0.2 ms (15%) to a warm read of `hyparquet_delta`
(1.2 against 1.4 ms on the same machine and run). `readRowsAt` of 256
scattered rows of a 1,000,000-row file with 262,144-row row groups of
1 MB pages, one DOUBLE column: 10 ms (0.0.1: 170 ms, one pass per run of
nearby rows; 1.1.0, with its page cache: 17 ms).

On small files pqparser is slower: each reader instantiates the module
(0.3 ms), and arrow-rs costs more per read. On large ones it is faster,
except whole columns of doubles (5 ms per million against 2 ms).
table-viewer's 2,048-row windows and scattered rows take about as long as
with 1.1.0.

## Licence

GPL-3.0-only, as `@jbearak/dta-parser`; see `LICENSE`. The crates
compiled into the module, and their licences, are in
`THIRD-PARTY-NOTICES`.
