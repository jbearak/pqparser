# AGENTS.md

pqparser is a read-only Parquet reader for Node: TypeScript over
arrow-rs compiled to wasm, with the Parquet API that `@jbearak/dta-parser`
1.1.0 gave table-viewer. See README.md.

## Rules that override everything else

These are dta-parser's rules, and apply here as there.

- Never run Stata. It is not installed on this machine; do not install it
  or look for another copy.
- Never read Stata's installed files: ado files, help files, or anything
  else that ships with Stata. Do not use tools or data extracted from them.
- Never use output Stata produced as a test oracle or expected result.
  Expected values come only from open-source libraries (pyarrow, haven,
  ReadStat, pandas and the like) and from `@jbearak/dta-parser` 1.1.0.
- Make fixtures without Stata: with dta-parser's writers, or with
  pyarrow, polars, arrow (R) or other open-source writers, from data you
  create. Do not use example datasets that ship with Stata.
- Take facts about the `.dta` and Parquet formats only from general
  knowledge and from open-source projects' documentation and code. Do
  not look up or use official Stata documentation, and never copy text
  from it; write in your own words. Copy open-source code only if its
  license allows, with credit.
- If you think you have found a bug in another project (arrow-rs,
  pyarrow, or any other), never file an issue, pull request or comment
  there without the user's approval of that specific report.

## Publishing

The package is private. Never publish it to npm or any other registry
without the user's approval.

## table-viewer

table-viewer uses this package. Read it only to learn how it reads
Parquet and `.dta` files and what it needs from this package. It is
read-only from here: never change it, push to it, or open pull requests
on it.
