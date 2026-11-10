// What pqparser accepts and refuses: options, closed readers, rows and
// variables out of range, nested columns, and the codecs it drops.
import assert from "node:assert/strict";
import { test } from "node:test";
import { join } from "node:path";
import { FIXTURES, OPTIONS, bytesOf, old, pq } from "./helpers.mjs";

const bytes = bytesOf(join(FIXTURES, "compat/pyarrow_nested.parquet"));
const code = (c) => (e) => e.code === c;

test("options: table-viewer's are accepted; nested \"skip\" and bad values are refused", () => {
  pq.openParquet(bytes, OPTIONS).close();
  pq.openParquet(bytes).close();
  assert.throws(() => pq.openParquet(bytes, { nested: "skip" }), (e) => e.code === "DTA_INVALID" && /nested must be "error"/.test(e.message));
  assert.throws(() => pq.openParquet(bytes, { int64: "text" }), code("DTA_INVALID"));
  assert.throws(() => pq.openParquet(bytes, { pageCacheBytes: -1 }), code("DTA_INVALID"));
});

test("errors carry 1.1.0's codes and classes", () => {
  const a = pq.openParquet(bytes, OPTIONS);
  const b = old.openParquet(bytes, OPTIONS);
  const same = (f) => {
    let want;
    try {
      f(b);
    } catch (e) {
      want = e;
    }
    assert.ok(want, "1.1.0 throws");
    assert.throws(() => f(a), (e) => e.code === want.code && e.constructor === want.constructor && (e.message === want.message || e.code !== "DTA_OUT_OF_RANGE"));
  };
  same((r) => r.readColumns([99]));
  same((r) => r.readColumns(["no such column"]));
  same((r) => r.readColumns([0], { start: -1 }));
  same((r) => r.readColumns([0], { start: 0, count: r.info.nobs + 1 }));
  same((r) => r.readRowsAt([r.info.nobs]));
  same((r) => r.readRows(0, 1)); // every column, nested among them
  same((r) => r.readColumns([r.variables.findIndex((v) => v.role === "nested")]));
  for (const r of [a, b]) r.close();
  same((r) => r.readColumns([0]));
  same((r) => r.valueLabels("x"));
});

test("brotli is not read: the error names it, and the footer still is", () => {
  for (const name of ["compat/pyarrow_brotli", "compat/fastparquet_int96_brotli", "compat/parquet2_brotli"]) {
    const r = pq.openParquet(bytesOf(join(FIXTURES, `${name}.parquet`)), OPTIONS);
    assert.ok(r.info.compression.includes("brotli"));
    const k = r.variables.findIndex((v) => v.columnKind !== null);
    assert.throws(() => r.readColumns([k]), (e) => e.code === "DTA_INVALID" && e.message === `column ${JSON.stringify(r.variables[k].name)}: brotli compression is not supported by pqparser`);
    assert.throws(() => r.readRowsAt([0], [k]), code("DTA_INVALID"));
    assert.deepEqual(r.readColumns([k], { start: 0, count: 0 })[0].missing?.length ?? 0, 0);
  }
});

test("missing values are dta-parser 2's objects, shared through the realm", () => {
  assert.equal(pq.MISSING, globalThis[Symbol.for("@jbearak/dta-parser/missing/v1")]);
  assert.equal(pq.missingValue(2), pq.MISSING[".a"]);
  assert.ok(pq.isMissing(pq.MISSING[".z"]) && !pq.isMissing(0));
});
