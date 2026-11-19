# Fixtures

Copied from dta-parser's `compat/parquet` at commit 0ab136d
(2026-11-10), with their READMEs, which say how each file was made and
credit the projects that wrote and read them:

- `compat/` is dta-parser's `compat/parquet/fixtures` and `expected`
  (README: `compat/README.md`): files other tools wrote, and what pyarrow
  reads from each.
- `legacy/` is `compat/parquet/legacy` (README: `legacy/README.md`):
  files R's and TypeScript's datakit wrote, and what pyarrow reads from
  each, its `dta` profile included.

The scripts and paths those READMEs name are in the dta-parser
repository, not here. No file here comes from Stata or from any
dataset; every value was made by those scripts.

`odd/` holds small files whose footers arrow-rs refuses and other readers
take, made by `scripts/make_odd.py` (pyarrow, then bytes of the footer
changed) and, for `null_text_and_wide_decimal.parquet`, by arrow-rs's
`SerializedFileWriter` (see `test/api.test.mjs`). `parquet-testing/` is
Apache's corpus (see its README). `not_utf8.parquet` and
`null_column.parquet` are described in the tests that read them.
