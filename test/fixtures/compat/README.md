# Parquet compatibility: fixtures and readback

Two checks for `crates/parquet` (plan 0018):

- **Fixtures** (`fixtures/`): small Parquet files written by other tools
  from data the scripts here make. `expected/` holds what pyarrow reads
  from each, as JSON. `crates/parquet/tests/compat.rs` reads every
  fixture with dta-parquet, whole and in row windows, on one thread and
  on four, and compares.
- **Readback**: files dta-parquet writes (from data made in
  `crates/parquet/tests/support/mod.rs`) are read by pyarrow, pandas,
  polars, DuckDB, arrow (R) and nanoparquet, and their values and types
  checked against what the writer was given and against plan 0018's
  table "What other tools see in our files".

A third set, [`legacy/`](legacy/README.md), holds files R's and
TypeScript's datakit wrote, with what datakit and pyarrow read from
them, so that a later reader can be checked against them.

No file here comes from Stata or from any dataset; every value is made by
the scripts.

## Fixtures

| file | writer | what it covers |
|---|---|---|
| `pyarrow_default.parquet` | pyarrow, defaults | snappy, dictionary and PLAIN, data page v1 |
| `pyarrow_v2_pages.parquet` | pyarrow, `data_page_version="2.0"` | data page v2 |
| `pyarrow_gzip.parquet` | pyarrow, `compression="gzip"` | gzip |
| `pyarrow_small_pages.parquet` | pyarrow, zstd, 200-row row groups, 1 KiB pages, `write_page_index=True` | row groups, many pages, an offset index |
| `pyarrow_plain.parquet` | pyarrow, `use_dictionary=False`, no compression | PLAIN only, uncompressed |
| `pyarrow_nested.parquet` | pyarrow, defaults | a list, a struct and a map among flat columns, which are not read |
| `polars_default.parquet` | polars, defaults | zstd, polars' encodings |
| `polars_zstd.parquet` | polars, zstd, statistics, 250-row row groups | row groups, polars' offset index |
| `duckdb_default.parquet` | DuckDB `COPY ... (FORMAT parquet)` | PLAIN_DICTIONARY, DuckDB's timestamps |
| `arrow_r_default.parquet` | arrow (R) `write_parquet`, defaults | factors, zoned `POSIXct`, list of raw |
| `nanoparquet_default.parquet` | nanoparquet `write_parquet`, defaults | nanoparquet's encodings |
| `pyarrow_brotli.parquet` | pyarrow, `compression="brotli"` | brotli |
| `pyarrow_delta.parquet` | pyarrow, no dictionary, `column_encoding`, no compression, 1 KiB pages | DELTA_BINARY_PACKED (INT32, INT64, decimals as integers), DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY (text and FIXED_LEN_BYTE_ARRAY), BYTE_STREAM_SPLIT (FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY), many pages |
| `pyarrow_delta_v2.parquet` | the same, data page v2, snappy, 250-row row groups, `write_page_index=True` | the same encodings in pages v2, row groups, an offset index |
| `pyarrow_types.parquet` | pyarrow, `use_deprecated_int96_timestamps=True` | INT96; TIME in ms, us and ns; DECIMAL(9, 2) to DECIMAL(50, 5) in FIXED_LEN_BYTE_ARRAY; unsigned 8 to 64 bits; fixed-size binary; UUID; JSON; a dictionary of integers |
| `pyarrow_types_v2.parquet` | the same table, `store_decimal_as_integer=True`, data page v2, zstd, small pages and row groups, an offset index | DECIMAL in INT32 and INT64, TIMESTAMP in place of INT96 |
| `fastparquet_default.parquet` | fastparquet `write`, 250-row row groups | pandas' unsigned and nullable types, a categorical, fastparquet's encodings |
| `fastparquet_int96_brotli.parquet` | fastparquet, `times="int96"`, `compression="BROTLI"` | INT96 and brotli from another writer |
| `parquet2_default.parquet` | Parquet2.jl `writefile`, defaults | Julia's types: unsigned integers, `Time` (TIME in ns), `DateTime`, a `Dec64` decimal (in INT64), bytes; snappy |
| `parquet2_zstd_pages.parquet` | Parquet2.jl, zstd, 7 pages a column | zstd, many pages |
| `parquet2_brotli.parquet` | Parquet2.jl, brotli, 3 pages a column | brotli from a third writer |
| `hyparquet_delta.parquet` | hyparquet-writer, an encoding chosen per column, no compression, 1 KiB pages, 250-row row groups | DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT from another writer; DECIMAL in a BYTE_ARRAY; ENUM; UINT_32; JSON; UUID |
| `hyparquet_default.parquet` | hyparquet-writer, defaults | the same types with its own encodings, snappy |

The Python fixtures hold 8-, 16-, 32- and 64-bit integers, floats and
doubles (with NaN and -0.0), booleans, text (repeated and distinct),
bytes, dates, timestamps in milliseconds, microseconds and nanoseconds
(UTC, a zone, and local) and a dictionary column, each with nulls. The R
fixtures hold R's types: integer, double, logical, character, `Date`,
`POSIXct` with a zone, factor and (arrow only) a list of raw vectors.

The second set (from `pyarrow_brotli` on) holds the types and
encodings plan 0018's second slice reads: unsigned integers, TIME,
INT96, DECIMAL (some values with more than 15 significant digits, up to
50 digits), fixed-size binary, UUID, JSON, ENUM, a dictionary of
integers, with nulls; in DELTA and BYTE_STREAM_SPLIT encodings, pages
v1 and v2, and brotli from three writers.

To regenerate, from the repository root:

    python3 compat/parquet/make_fixtures.py    # pyarrow, polars, DuckDB, fastparquet
    Rscript compat/parquet/make_fixtures.R     # arrow, nanoparquet
    julia --project=ENV compat/parquet/make_fixtures.jl    # Parquet2.jl (ENV has Parquet2, DataFrames, DecFP)
    node compat/parquet/make_fixtures.mjs DIR/node_modules/hyparquet-writer
    python3 compat/parquet/expected.py         # pyarrow reads them all

`python3 compat/parquet/make_fixtures.py breadth` writes only the
pyarrow and fastparquet files of the second set. hyparquet-writer is
not a dependency of this repository: install it in a scratch directory
(`npm install hyparquet-writer`) and pass its path.

The fixtures were written with pyarrow 26.0.1, polars 2.1.0, DuckDB
2.0.0, arrow (R) 26.0.0, nanoparquet 0.5.2, fastparquet 2026.9.0 (with
pandas 3.1.1), Parquet2.jl 0.2.37 (Julia 1.13.3) and hyparquet-writer
0.18.0 (Node 26).

Expected values are pyarrow's, decimals as text with every digit. Two
readings differ from pyarrow's by design (plan 0018): pyarrow reads an
ENUM column as binary, ours as text (the compat test compares their
bytes); and a text column `ARROW:schema` marks as a dictionary is also
read as a categorical whose levels are the rows' values in order of
first appearance, which the test checks against pyarrow's values.

## Readback

    cargo run -p dta-parquet --example samples -- DIR
    python3 compat/parquet/readback.py DIR     # pyarrow, pandas, polars, DuckDB
    Rscript compat/parquet/readback.R DIR      # arrow, nanoparquet

`samples` writes every type with nulls, `.a` to `.z`, categoricals
(ordered and not, with an unused level) and time zones; the same in each
codec; 5,000 rows in five row groups of small pages; an empty table;
`.a` to `.z` in a byte, an int, a long, a float and a double column,
beside `.` and a NaN value, written once as tagged NaNs and once with
`missing_letters` off; the same for a date and three timestamps (a zone,
an instant without one, a local time in nanoseconds), which are written
as days and seconds since 1970 in DOUBLE columns, with `logical` in the
profile; and 5,000 columns with 50 label sets of 1,000
labels. Each script checks every
value, that the `dta` key is in the file's metadata, and these cells of
plan 0018's table:

| ours | pyarrow / pandas | polars | DuckDB | arrow (R) | nanoparquet |
|---|---|---|---|---|---|
| `.a` to `.z` | pyarrow: NaN, not null, in a `double` column, with the storage or logical type in the profile; pandas: missing (`isna()`) | NaN, not null (`null_count()` leaves them out) | NaN, not NULL: `count()` counts them, and `x IS NULL OR isnan(x)` finds every missing value | tagged NA, letter kept (`haven::na_tag()`) | tagged NA, letter kept |
| categorical | `dictionary<..., ordered>` with every level in order; pandas `category`, ordered kept | `Categorical` | VARCHAR | factor (ordered kept), every level | factor, every level, never ordered |
| zoned timestamp | tz-aware, our zone | our zone | TIMESTAMP WITH TIME ZONE | `POSIXct` in our zone | `POSIXct` in UTC |
| INT(8), INT(16) | int8, int16 (pandas: float64 with nulls) | Int8, Int16 | TINYINT, SMALLINT | integer | integer |

Results with pyarrow 26.0.1, pandas 3.1.1, polars 2.1.0, DuckDB 2.0.0,
arrow (R) 26.0.0 and nanoparquet 0.5.2: every check passes, with these
differences from plan 0018's table, none of which is a bug in
those tools:

- nanoparquet 0.5.2 reads our categoricals as factors with our levels
  (from `ARROW:schema`), not as character as the table says; it does not
  keep whether they are ordered.
- nanoparquet 0.5.2 does not read LZ4_RAW ("Compression is not supported
  yet"), so the readback skips that file for it.
- A table with no rows has no row groups, so no dictionary pages: other
  tools see its categoricals with no levels. Ours read them from the
  profile.

### hyparquet reads the npm package's files

    node compat/parquet/readback.mjs bench/node/node_modules

writes, with the npm package's `writeParquet`, a table of every type it
writes (byte, int, long, float, double with NaN, `.a` to `.z`, int64
past 2^53, bool, date, zoned and local timestamps, text, binary and an
ordered categorical with an unused level), with nulls, in every codec,
in small row groups, and with `missingLetters: false`; hyparquet 1.32.0
with hyparquet-compressors 1.1.3 (`bench/node`) reads each file and
every value is checked against what was written, by hyparquet's own
rules (INT64 as bigint, DATE and TIMESTAMP as `Date`s in milliseconds,
`.a` to `.z` as NaN). Result: 315,000 values checked, none differ.

## Across languages

    sh compat/parquet/cross_language.sh [DIR]

R's datakit and TypeScript's each write a file (`cross_language.R
write`, `from_r.parquet`; `cross_language.mjs write`,
`from_typescript.parquet`), and each reads the other's and checks it
cell by cell. Both scripts make the same literal data, so each checks
every row's value and missing code (`.` or which of `.a` to `.z`), the
variable labels, display formats, value label sets (with labels on
letters), the data label and notes. The data has a double, a byte, a
long and a float column holding letters (the long one written as DOUBLE
with `"storage": "long"`), an integer with nulls, text, a factor with an
unused level, dates with a format, zoned times, and a byte column
labelled by a set where two codes share a text. R's file also has the
exactness edges: a `Date` with a fraction of a day, which TypeScript
reads as the DOUBLE count it is written as, and a `POSIXct` whose
seconds no whole number of microseconds gives back. TypeScript's file
adds a date and a timestamp holding letters, which R reads as `Date` and
`POSIXct` with tagged NAs, and an ordered categorical holding letters
(plan 0022), which R reads as labelled codes with the letters, with
`factors = TRUE` too.

One value had to change while writing the scripts: R parses the literal
`1e300` as `1.0000000000000006e300` (its own string-to-double conversion
is not correctly rounded), so the scripts use `2^1000`, which both
languages hold exactly.

    sh compat/parquet/categorical_marker.sh [DIR]

TypeScript's datakit saves two categoricals as `.dta` with the
`dtaparser_categorical` marker (plan 0023), one of them ordered and
holding `.a` and `.z`. R's dtaparser reads them without a warning: the
first as a factor, and the second as labelled numbers with its letters,
with `factors = TRUE` too.

## Credits

The fixtures were written with, and the expected values read by, these
open-source projects:

- [Apache Arrow](https://arrow.apache.org/) (pyarrow and the arrow R
  package), Apache-2.0
- [polars](https://pola.rs/), MIT
- [DuckDB](https://duckdb.org/), MIT
- [nanoparquet](https://github.com/r-lib/nanoparquet), MIT
- [fastparquet](https://github.com/dask/fastparquet), Apache-2.0
- [Parquet2.jl](https://gitlab.com/ExpandingMan/Parquet2.jl), MIT, with
  [DecFP.jl](https://github.com/JuliaMath/DecFP.jl), BSD-2-Clause, for its
  decimals
- [hyparquet-writer](https://github.com/hyparam/hyparquet-writer), MIT,
  and [hyparquet](https://github.com/hyparam/hyparquet) and
  [hyparquet-compressors](https://github.com/hyparam/hyparquet-compressors),
  MIT, for the npm package's readback
- [pandas](https://pandas.pydata.org/), BSD-3-Clause, for the readback
- [haven](https://haven.tidyverse.org/), MIT, for the readback's tagged NAs
