# parquet-testing

Files from Apache Parquet's test corpus,
[apache/parquet-testing](https://github.com/apache/parquet-testing) at
commit 0e00127 (2026-11-16), unchanged: every file in `data/` except the
four largest (`alltypes_tiny_pages_plain`, `overflow_i16_page_cnt`,
`alltypes_tiny_pages` and `lz4_raw_compressed_larger`, 0.4 to 0.8 MB
each, whose features smaller files here cover), and
`bad_data/ARROW-GH-43605.parquet`, the one file in `bad_data/` pyarrow
reads. They are under the Apache License 2.0 (`LICENSE.txt`); their
descriptions are in that repository's `data/README.md`.

`expected/` holds what pyarrow 26.0.1 reads from each, written by
`python3 scripts/expected.py test/fixtures/parquet-testing`.
`test/corpus.test.mjs` reads every file with pqparser as table-viewer
opens it and compares.
