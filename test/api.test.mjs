// What pqparser accepts and refuses: options, closed readers, rows and
// variables out of range, nested columns, and text that is not UTF-8.
import assert from "node:assert/strict";
import { test } from "node:test";
import { join } from "node:path";
import { FIXTURES, OPTIONS, bytesOf, old, plain, pq } from "./helpers.mjs";

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

// test/fixtures/not_utf8.parquet, written by pyarrow 26.0.1 from
// `a = pa.array(vals, pa.binary()).view(pa.string())` as columns "plain",
// "coded" (`use_dictionary=["coded"]`) and "json"
// (`pa.ExtensionArray.from_storage(pa.json_(), a)`), with vals
// [b"ok", b"caf\xe9", None, b"ends \xe2\x82", b"\xac starts", b"caf\xe9", b"\xff\xfe"].
// 1.1.0 refuses the file's reads.
test("text that is not UTF-8 reads with each bad byte replaced, value by value", () => {
  const bytes = bytesOf(join(FIXTURES, "not_utf8.parquet"));
  const want = ["ok", "caf\ufffd", "", "ends \ufffd", "\ufffd starts", "caf\ufffd", "\ufffd\ufffd"];
  // JSON, which arrow-rs does not check, read first and alone.
  const json = pq.openParquet(bytes, OPTIONS);
  assert.deepEqual(json.readColumns(["json"])[0].values, want);
  json.close();
  const r = pq.openParquet(bytes, OPTIONS);
  for (const c of r.readColumns([0, 1, 2])) {
    assert.deepEqual(c.values, want);
    assert.deepEqual([...c.missing], [0, 0, 1, 0, 0, 0, 0]);
  }
  assert.deepEqual(r.readRowsAt([6, 1], [1, 0]), [[want[6], want[6]], [want[1], want[1]]]);
  r.close();
});

// test/fixtures/odd/null_text_and_wide_decimal.parquet, written by
// arrow-rs's SerializedFileWriter from `optional binary u (UNKNOWN)` (3
// nulls) and `required fixed_len_byte_array(17) d (DECIMAL(15, 2))` (12.34,
// -0.05, 999999999999.99). pyarrow refuses a DECIMAL in 17 bytes.
test("a BYTE_ARRAY of nulls only, and a DECIMAL of 15 digits in 17 bytes, read as 1.1.0 reads them", () => {
  const bytes = bytesOf(join(FIXTURES, "odd/null_text_and_wide_decimal.parquet"));
  const r = pq.openParquet(bytes, OPTIONS);
  const [u, d] = r.readColumns([0, 1]);
  assert.deepEqual([...u.missing], [1, 1, 1]);
  assert.deepEqual([...d.values], [12.34, -0.05, 999999999999.99]);
  const b = old.openParquet(bytes, OPTIONS);
  assert.deepEqual(plain(r.readColumns([0, 1])), plain(b.readColumns([0, 1])));
});

test("a file that does not end in PAR1 does not open", () => {
  const bytes = Buffer.from(bytesOf(join(FIXTURES, "compat/pyarrow_default.parquet")));
  bytes.write("BAD!", bytes.length - 4);
  assert.throws(() => pq.openParquet(bytes, OPTIONS), code("DTA_CORRUPT"));
});

test("missing values are dta-parser 2's objects, shared through the realm", () => {
  assert.equal(pq.MISSING, globalThis[Symbol.for("@jbearak/dta-parser/missing/v1")]);
  assert.equal(pq.missingValue(2), pq.MISSING[".a"]);
  assert.ok(pq.isMissing(pq.MISSING[".z"]) && !pq.isMissing(0));
});
