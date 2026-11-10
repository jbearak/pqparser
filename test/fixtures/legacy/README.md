# Legacy Parquet fixtures

Files that R's datakit 3.1.0 and the npm package `@jbearak/datakit`
2.0.0 wrote, kept so that a later reader can be tested against them
after these writers are gone. Each file comes with what other readers
make of it:

| file | what it is |
|---|---|
| `<name>.parquet` | the file |
| `<name>.read.rds` | what R's datakit 3.1.0 read from it (`readings.R`) |
| `expected/<name>.json` | what pyarrow 26.0.1 read from it (`expected.py`) |
| `r_types.rds`, `r_empty.rds`, `r_edges.rds`, `r_rows.rds` | the data frames R wrote |

TypeScript's data is in `make.mjs`.

No file here comes from Stata or from any dataset; every value is made
by the scripts.

## The files

| file | writer | what it covers |
|---|---|---|
| `r_types` | R, defaults (snappy) | every type R writes: labelled doubles, bytes and ints holding `.a` to `.z`, a long and a float holding letters, two codes sharing a text, logical, text (empty, accented, CJK, long), factor with an unused level, ordered factor, `Date` (with letters, with a fraction of a day), `POSIXct` in a zone, in UTC and with a letter, integer64, raw vectors; variable labels, formats, the data label, notes, characteristics, sort order |
| `r_types_no_letters` | the same, `missing_letters = FALSE` | letters written as nulls |
| `r_empty` | `r_types` with no rows | no row groups: levels and labels from the profile alone |
| `r_edges` | R | int64 values a double does not hold exactly |
| `r_rows_zstd`, `_gzip`, `_lz4_raw`, `_none` | R, each codec, 300- or 500-row row groups | 1,200 rows: labels with `.a` to `.z`, text with few and many distinct values, a factor, a zoned timestamp |
| `ts_types` | TypeScript, defaults | every type TypeScript writes: the same as R's, plus unsigned 8, 16 and 32 bits, timestamps in ms and ns without a zone, a date and a timestamp holding letters, and an ordered categorical holding `.a` to `.z` (DOUBLE level numbers, plan 0022) |
| `ts_types_no_letters` | the same, `missingLetters: false` | letters written as nulls |
| `ts_edges` | TypeScript | int64 values a double does not hold exactly, and nanoseconds below a microsecond |
| `ts_rows_zstd`, `ts_rows_lz4_raw` | TypeScript, 300- or 500-row row groups | as R's, plus a categorical holding letters |

## The readings

`readings.R` reads each file with `read_parquet_info()` and with
`read_parquet()` seven ways: with the defaults, `factors = TRUE`,
`int64 = "integer64"`, `int64 = "character"`, `int64 = "integer64"`
with `timestamps = "truncate"`, a window of rows (a third skipped, then
half the rows) and the last two columns in reverse order. It keeps each
value with its warnings, or the error; the `_edges` files' default
readings are errors by design. datakit's `test-legacy.R` checks that
the reader still gives every reading, bit for bit.

In pyarrow's JSON, a NaN is `"NaN:"` and its bits in hexadecimal, so
`.a` to `.z` keep their letter. The `dta` entry is the profile
(`docs/parquet.md`), parsed.

## Making them again

The files are made once and committed: the writers will be deleted, so
the files are not made again in CI. To make them again, from the
repository root, with `js/` and `js/datakit/` built, dtaparser and
datakit installed, and pyarrow and numpy importable:

    sh compat/parquet/legacy/make.sh

Two runs give the same bytes.
