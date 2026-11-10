// table-viewer's calls, replayed: what its Parquet source (src/data-source/
// parquet-source.ts and file-parquet-source.ts) asks of a reader, in its
// order, from opening by file descriptor to closing, against pqparser and
// against @jbearak/dta-parser 1.1.0. Every answer must be the same.
import assert from "node:assert/strict";
import { closeSync, fstatSync, openSync, readSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { FIXTURES, OPTIONS, bytesOf, old, oldNode, plain, pq, pqNode, rowsOf } from "./helpers.mjs";

const BLOCK_ROWS = 2048;
const WINDOW_ROWS = 256;
const BATCH_COLUMNS = 16;
const REPEATING = new Set(["RLE_DICTIONARY", "PLAIN_DICTIONARY", "DELTA_BYTE_ARRAY"]);

/** Everything table-viewer reads from `reader`, as plain data. `skip(v)` leaves a variable out of the table. */
function session(reader, skip = () => false) {
  const out = {};
  const { info, variables } = reader;
  // assert_parquet_limits
  out.limits = {
    nobs: info.nobs,
    columns: variables.length,
    chunks: variables.length * info.rowGroups.length,
    profile: info.keyValue.find(({ key }) => key === "dta")?.bytes,
    uncompressed: info.rowGroups.reduce((n, g) => n + g.uncompressedBytes, 0),
    dictionaries: variables.filter((v) => v.columnKind !== null).map((v) => v.chunks.map((c) => c?.dictionaryPageBytes ?? 0)),
  };
  const readable = [];
  variables.forEach((v, index) => {
    if (v.columnKind !== null && !skip(v)) readable.push(index);
  });
  // assert_value_label_limits, then the column plans and their tooltips
  const labels = (name) => (name ? reader.valueLabels(name) : undefined);
  out.labels = [...new Set(readable.map((k) => variables[k].valueLabels))].map((name) => [name, plain([...(labels(name) ?? [])])]);
  out.plans = readable.map((k) => {
    const v = variables[k];
    return plain({
      name: v.name,
      columnKind: v.columnKind,
      type: v.type,
      unit: v.unit,
      timeZone: v.timeZone,
      format: v.format,
      label: v.label,
      valueLabels: v.valueLabels,
      physicalType: v.physicalType,
      logicalType: v.logicalType,
      role: v.role,
      chunks: v.chunks.map((c) =>
        c === null
          ? null
          : {
              uncompressedBytes: c.uncompressedBytes,
              dictionaryPageBytes: c.dictionaryPageBytes,
              repeats: c.dictionaryPageBytes !== null || c.encodings.some((e) => REPEATING.has(e)),
              codec: c.codec,
              nullCount: c.nullCount,
              min: c.min,
              max: c.max,
            },
      ),
    });
  });
  // parquet_warnings
  out.leftOut = variables.filter((v) => v.columnKind === null).map((v) => [v.name, v.role, v.notRead]);
  out.warnings = info.warnings;
  out.groupStarts = info.rowGroups.map((g) => g.firstRow);
  // Rendered windows: blocks of rows, columns in batches.
  out.blocks = [];
  for (let start = 0; start < info.nobs; start += BLOCK_ROWS) {
    const count = Math.min(BLOCK_ROWS, info.nobs - start);
    for (let b = 0; b < readable.length; b += BATCH_COLUMNS) {
      const batch = readable.slice(b, b + BATCH_COLUMNS);
      out.blocks.push(reader.readColumns(batch, { start, count }).map(rowsOf));
    }
  }
  // A column read alone for one window (read_bounded).
  if (readable.length && info.nobs) {
    const start = Math.floor(info.nobs / 2);
    out.alone = reader.readColumns([readable[readable.length - 1]], { start, count: Math.min(WINDOW_ROWS, info.nobs - start) }).map(rowsOf);
  }
  // Sparse reads: sorted unique rows, grouped within a block's span.
  const rows = [...new Set([0, 3, 7, 300, 301, 299, 1000, 1999, 2047, info.nobs - 1].filter((r) => r >= 0 && r < info.nobs))].sort((a, b) => a - b);
  out.sparse = [];
  for (let i = 0; i < rows.length; ) {
    const group = rows.filter((r) => r >= rows[i] && r - rows[i] + 1 <= BLOCK_ROWS);
    i += group.length;
    for (let b = 0; b < readable.length; b += BATCH_COLUMNS) out.sparse.push(plain(reader.readRowsAt(group, readable.slice(b, b + BATCH_COLUMNS))));
  }
  reader.close();
  return out;
}

/** Open as file-parquet-source.ts does: its own descriptor, the footer length checked first. */
function openByDescriptor(open, path) {
  const fd = openSync(path, "r");
  try {
    const size = fstatSync(fd).size;
    const tail = new Uint8Array(8);
    readSync(fd, tail, 0, 8, size - 8);
    assert.equal(String.fromCharCode(...tail.subarray(4)), "PAR1");
    return { reader: open(fd, OPTIONS), close: () => closeSync(fd) };
  } catch (e) {
    closeSync(fd);
    throw e;
  }
}

const FILES = ["legacy/r_types", "legacy/ts_rows_zstd", "legacy/ts_types", "compat/pyarrow_small_pages", "compat/polars_zstd", "compat/arrow_r_default", "compat/pyarrow_nested"];

for (const name of FILES) {
  test(`table-viewer's session on ${name}`, () => {
    const path = join(FIXTURES, `${name}.parquet`);
    const a = openByDescriptor(pqNode.openParquetFile, path);
    const b = openByDescriptor(oldNode.openParquetFile, path);
    try {
      assert.deepEqual(session(a.reader), session(b.reader));
    } finally {
      a.close();
      b.close();
    }
  });
}

test("table-viewer's session on bytes (git: URIs and compare originals)", () => {
  const bytes = bytesOf(join(FIXTURES, "legacy/r_rows_gzip.parquet"));
  assert.deepEqual(session(pq.openParquet(bytes, OPTIONS)), session(old.openParquet(bytes, OPTIONS)));
});

test("table-viewer's session where a DECIMAL has more than 15 digits: the column is left out and named", () => {
  const bytes = bytesOf(join(FIXTURES, "compat/pyarrow_types.parquet"));
  const wide = (v) => v.type === "decimal" && v.columnKind === "string";
  const a = session(pq.openParquet(bytes, OPTIONS));
  const b = session(old.openParquet(bytes, OPTIONS), wide);
  const names = ["dec18", "dec38", "dec50"];
  assert.deepEqual(a.leftOut.map(([n]) => n), names);
  for (const [, role, why] of a.leftOut) {
    assert.equal(role, "data");
    assert.match(why, /^column "dec\d\d" is DECIMAL\(\d\d, \d+\), with more digits than a number holds, which is not read$/);
  }
  // 1.1.0's limits count those columns' dictionary pages; the rest is the same.
  delete a.limits.dictionaries;
  delete b.limits.dictionaries;
  assert.deepEqual({ ...a, leftOut: [] }, { ...b, leftOut: [] });
});
