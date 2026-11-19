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
import { OPTIONS, bytesOf, cell, fixtures, parseExact, pq, same } from "./helpers.mjs";

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
    let checked = 0;
    for (const col of expected.columns) {
      const k = r.variables.findIndex((v) => v.name === col.name);
      const v = r.variables[k];
      if (/^dictionary<values=string/.test(col.arrow_type)) assert.equal(v.columnKind, "dictionary", `${col.name} is a categorical`);
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
    assert.ok(checked > 0);
    r.close();
  });
}
