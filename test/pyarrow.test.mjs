// pqparser against what pyarrow reads from each fixture (the JSON in
// test/fixtures/*/expected, made by the monorepo's compat/parquet scripts),
// by the rules of its crates/parquet/tests/compat.rs: timestamps and times
// as integers in their unit, dates as days, bytes as lists of integers,
// decimals as text, NaN as "NaN" (in the legacy files "NaN:" and its bits,
// so `.a` to `.z` keep their letter). Two readings differ by design: an
// ENUM is text here (pyarrow: binary), and a dictionary of text is a
// categorical whose levels are the rows' values.
import assert from "node:assert/strict";
import { test } from "node:test";
import { OPTIONS, bytesOf, fixtures, pq, usesBrotli } from "./helpers.mjs";

const UNITS = ["s", "ms", "us", "ns"];

/** Parse JSON keeping integers beyond the safe ones exact, as bigints. */
function parseExact(text) {
  return JSON.parse(text, (_, v, ctx) => (typeof v === "number" && Number.isInteger(v) && !Number.isSafeInteger(v) && /^-?\d+$/.test(ctx?.source ?? "") ? BigInt(ctx.source) : v));
}

/** The bits of a double as 16 hex digits. */
function bits(x) {
  const d = new DataView(new ArrayBuffer(8));
  d.setFloat64(0, x);
  return d.getBigUint64(0).toString(16).padStart(16, "0");
}

/** The tagged NaN of missing code 2 (`.a`) to 27 (`.z`), as pyarrow prints its bits. */
function taggedBits(code) {
  return (0x7ff0000000000000n | (BigInt(0x61 + code - 2) << 32n) | 0x7a2n).toString(16).padStart(16, "0");
}

/** Row `i` of our column: `{ code }` for a missing number, null for a null, else the value. */
function cell(c, i) {
  switch (c.kind) {
    case "number":
      return c.missing[i] ? { code: c.missing[i] } : c.values[i];
    case "bigint":
      return c.missing[i] ? null : c.values[i];
    case "dictionary":
      return c.missing[i] ? null : c.values[c.codes[i]];
    default:
      return c.missing?.[i] ? null : c.values[i];
  }
}

/** Whether pyarrow's `want` and our `got` are the same value, for a column of Arrow type `type`. */
function same(want, got, type, variable) {
  const missingCode = got !== null && typeof got === "object" && "code" in got ? got.code : 0;
  if (want === null) return got === null || missingCode === 1;
  if (typeof want === "string" && want.startsWith("NaN:")) {
    const b = want.slice(4);
    if (b.length === 16 && missingCode >= 2) return b === taggedBits(missingCode);
    return typeof got === "number" && Number.isNaN(got) && (b.length !== 16 || bits(got) === b);
  }
  if (missingCode) return false;
  if (want === "NaN") return typeof got === "number" && Number.isNaN(got);
  if (type === "bool") return got === (want ? 1 : 0);
  if (/^decimal/.test(type)) return got === Number(want);
  const unit = /^(?:timestamp|time\d\d)\[(\w+)/.exec(type)?.[1];
  if (unit) {
    // Our values count the Parquet unit; pyarrow's the Arrow type's (ARROW:schema may give another).
    const shift = UNITS.indexOf(variable.unit) - UNITS.indexOf(unit);
    const scaled = BigInt(want) * 1000n ** BigInt(Math.max(0, shift));
    const ours = BigInt(got) * 1000n ** BigInt(Math.max(0, -shift));
    return scaled === ours;
  }
  if (Array.isArray(want)) {
    const bytes = typeof got === "string" ? Buffer.from(got, "utf8") : got; // an ENUM is text here
    return bytes instanceof Uint8Array && Buffer.compare(Buffer.from(bytes), Buffer.from(want)) === 0;
  }
  if (typeof want === "bigint" || (typeof want === "number" && (typeof got === "bigint" || Number.isInteger(want)))) {
    return (typeof got === "number" || typeof got === "bigint") && BigInt(got) === BigInt(want);
  }
  return Object.is(want, got);
}

for (const f of fixtures()) {
  test(`${f.set}/${f.name}: as pyarrow reads it`, () => {
    const expected = parseExact(f.text);
    const r = pq.openParquet(bytesOf(f.path), OPTIONS);
    assert.equal(r.info.nobs, expected.rows);
    // Warnings only for categoricals of DOUBLE level numbers (datakit's plan
    // 0022, newer than 1.1.0, which reads them as numbers, as here).
    for (const w of r.info.warnings) assert.match(w, /"columns\.\w+\.logical"|levels for column "\w+", which is not text/);
    if (expected.row_groups) assert.deepEqual(r.info.rowGroups.map((g) => g.rows), expected.row_groups);
    const nested = expected.nested ?? [];
    for (const name of nested) assert.equal(r.variables.find((v) => v.name === name)?.role, "nested", name);
    assert.equal(r.variables.length, expected.columns.length + nested.length);
    const brotli = usesBrotli(r);
    let checked = 0;
    for (const col of expected.columns) {
      const k = r.variables.findIndex((v) => v.name === col.name);
      const v = r.variables[k];
      if (v.columnKind === null) {
        // DECIMAL with more than 15 digits: not read, on purpose.
        assert.match(col.arrow_type, /^decimal/, col.name);
        assert.ok(+/\((\d+)/.exec(col.arrow_type)[1] > 15, col.name);
        continue;
      }
      if (/^dictionary<values=string/.test(col.arrow_type)) assert.equal(v.columnKind, "dictionary", `${col.name} is a categorical`);
      if (brotli) continue;
      const [c] = r.readColumns([k]);
      assert.equal(col.values.length, r.info.nobs);
      for (let i = 0; i < col.values.length; i++) {
        const got = cell(c, i);
        assert.ok(same(col.values[i], got, col.arrow_type, v), `${col.name} row ${i + 1}: pyarrow ${JSON.stringify(col.values[i], (_, x) => (typeof x === "bigint" ? `${x}` : x))}, ours ${typeof got === "bigint" ? got : JSON.stringify(got)} (${col.arrow_type})`);
      }
      checked++;
    }
    // The legacy files' dta profile, as pyarrow found it: labels, formats and value labels.
    const dta = expected.dta;
    if (dta) {
      assert.equal(r.info.label, dta.label ?? "");
      assert.deepEqual(r.info.notes, dta.notes ?? []);
      for (const [name, entry] of Object.entries(dta.columns ?? {})) {
        const v = r.variables.find((x) => x.name === name);
        assert.equal(v.label, entry.label ?? "", `${name} label`);
        assert.equal(v.format, entry.format ?? "", `${name} format`);
        if (entry.categorical && v.columnKind === "dictionary") assert.deepEqual(v.categorical, { levels: entry.categorical.levels, ordered: entry.categorical.ordered ?? false });
      }
      for (const [name, pairs] of Object.entries(dta.value_labels ?? {})) {
        const labels = r.valueLabels(name);
        assert.deepEqual([...labels].map(([code, text]) => [typeof code === "object" ? code.missing : code, text]), pairs, `value labels ${name}`);
      }
    }
    assert.ok(brotli || checked > 0);
    r.close();
  });
}
