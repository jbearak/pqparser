// Apache Parquet's test corpus (test/fixtures/parquet-testing): every file
// opens, every flat column is read, and its values are pyarrow's, by the
// rules of pyarrow.test.mjs. The exceptions are listed here, each with
// what pqparser does instead and why.
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { FIXTURES, OPTIONS, cell, parseExact, pq, same } from "./helpers.mjs";

const DIR = join(FIXTURES, "parquet-testing");

/** Files whose reading differs from pyarrow's, and what is checked instead. */
const EXCEPTIONS = {
  // pyarrow 26 reads no rows; the file's description says 1, 2, 3.
  empty_data_pages_v1: (r) => assert.deepEqual([...r.readColumns([0])[0].values], [1n, 2n, 3n]),
  empty_data_pages_v2: (r) => assert.deepEqual([...r.readColumns([0])[0].values], [1n, 2n, 3n]),
  // pyarrow does not read ALP; each ALP column holds its plain column's values.
  "alp_extended.zstd": (r) => {
    for (const v of r.variables.filter((x) => x.name.includes("_alp_"))) {
      const plain = v.name.replace(/_alp_\d+$/, "_plain");
      assert.deepEqual(r.readColumns([v.name])[0], r.readColumns([plain])[0], v.name);
    }
  },
  // TIMESTAMP on FIXED_LEN_BYTE_ARRAY(12), which pyarrow refuses: the
  // annotation is dropped with a warning and the bytes read.
  flba12_timestamp: (r) => {
    assert.equal(r.info.warnings.length, 3);
    for (const v of r.variables) assert.equal(v.type, "binary");
    assert.deepEqual([...r.readColumns(["timestamp_millis"])[0].values[1]], [232, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
  },
  // pyarrow refuses these maps; they are nested, so not read here.
  incorrect_map_schema: (r) => assert.deepEqual(r.variables.map((v) => v.role), ["nested"]),
  "large_string_map.brotli": (r) => assert.deepEqual(r.variables.map((v) => v.role), ["nested"]),
  // Spark's INT96 timestamps after 2262, which pyarrow wraps around:
  // nanoseconds since 1970 do not hold them, and the read says so.
  int96_from_spark: (r) =>
    assert.throws(() => r.readColumns([0]), (e) => e.code === "DTA_INVALID" && /the INT96 timestamp in row 3 is 253402225200 seconds from 1970/.test(e.message)),
  // Its dictionary pages run past their column chunks, which pyarrow
  // tolerates and arrow-rs (and dta-parser 1.1.0) do not.
  "nation.dict-malformed": (r) => assert.throws(() => r.readColumns(["name"]), (e) => e.code === "DTA_CORRUPT"),
};

/** Warnings a file is expected to give. */
const WARNINGS = {
  int32_with_uuid_logical_type: [/^column "int32_uuid": its annotation does not fit its physical type, which is read instead \(Cannot annotate Uuid from INT32/],
};

const files = readdirSync(DIR).filter((f) => f.endsWith(".parquet")).sort();

test("the corpus is all here", () => assert.equal(files.length, 70));

for (const file of files) {
  const name = file.slice(0, -".parquet".length);
  test(`parquet-testing/${name}`, () => {
    const want = parseExact(readFileSync(join(DIR, "expected", `${name}.json`), "utf8"));
    const r = pq.openParquet(readFileSync(join(DIR, file)), OPTIONS);
    for (const v of r.variables) if (v.role !== "nested") assert.notEqual(v.columnKind, null, `${v.name}: ${v.notRead}`);
    const check = EXCEPTIONS[name];
    if (check) {
      check(r);
      return;
    }
    assert.equal(want.error, undefined, "pyarrow reads it");
    const warnings = WARNINGS[name] ?? [];
    assert.equal(r.info.warnings.length, warnings.length, r.info.warnings.join("\n"));
    warnings.forEach((w, k) => assert.match(r.info.warnings[k], w));
    assert.equal(r.info.nobs, want.rows);
    for (const n of want.nested ?? []) assert.equal(r.variables.find((v) => v.name === n)?.role, "nested", n);
    assert.equal(r.variables.length, want.columns.length + (want.nested ?? []).length);
    for (const col of want.columns) {
      const v = r.variables.find((x) => x.name === col.name);
      const [c] = r.readColumns([col.name]);
      for (let i = 0; i < col.values.length; i++) {
        const got = cell(c, i);
        assert.ok(same(col.values[i], got, col.arrow_type, v), `${col.name} row ${i + 1}: pyarrow ${JSON.stringify(col.values[i], (_, x) => (typeof x === "bigint" ? `${x}` : x))}, ours ${typeof got === "bigint" ? got : JSON.stringify(got)} (${col.arrow_type})`);
      }
    }
    r.close();
  });
}
