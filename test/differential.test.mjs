// pqparser against @jbearak/dta-parser 1.1.0, fixture by fixture: what
// table-viewer reads (info, variables, value labels, columns in windows,
// scattered rows) must be the same, value for value and missing code for
// missing code. The differences made on purpose are asserted as such.
import assert from "node:assert/strict";
import { test } from "node:test";
import { OPTIONS, bytesOf, fixtures, old, plain, pq, rowsOf, usesBrotli, variableOf } from "./helpers.mjs";

/** Variables pqparser does not read on purpose: DECIMAL with more than 15 digits, which 1.1.0 read as text. */
function wideDecimal(v) {
  return v.type === "decimal" && v.columnKind === "string";
}

/** Row windows: the start, across row groups, and the end. */
function windows(nobs, groups) {
  const out = [[0, nobs], [0, Math.min(1, nobs)], [Math.max(0, nobs - 3), Math.min(3, nobs)]];
  for (const g of groups.slice(1)) {
    const s = Math.max(0, g.firstRow - 7);
    out.push([s, Math.min(nobs - s, 20)]);
  }
  if (nobs > 10) out.push([Math.floor(nobs / 3), Math.floor(nobs / 2)]);
  return out;
}

/** Rows spread over the file, with repeats, out of order. */
function scattered(nobs) {
  if (nobs === 0) return [];
  const rows = [nobs - 1, 0, Math.floor(nobs / 2), 1 % nobs, Math.floor(nobs / 2), Math.floor(nobs / 7)];
  for (let r = 3; r < nobs; r += 97) rows.push(r);
  return rows;
}

for (const f of fixtures()) {
  test(`${f.set}/${f.name}: as 1.1.0 reads it`, () => {
    const bytes = bytesOf(f.path);
    const a = pq.openParquet(bytes, OPTIONS);
    const b = old.openParquet(bytes, OPTIONS);
    assert.deepEqual(plain(a.info), plain(b.info), "info");
    assert.equal(a.variables.length, b.variables.length);
    const readable = [];
    a.variables.forEach((v, k) => {
      const w = b.variables[k];
      if (wideDecimal(w)) {
        assert.equal(v.columnKind, null, `${v.name}: not read`);
        assert.match(v.notRead, /with more digits than a number holds/);
        const { columnKind, notRead, ...rest } = variableOf(v);
        const { columnKind: c2, ...want } = variableOf(w);
        assert.equal(c2, "string");
        assert.deepEqual(rest, want, v.name);
        return;
      }
      assert.deepEqual(variableOf(v), variableOf(w), v.name);
      if (v.columnKind !== null) readable.push(k);
    });
    // Every set a variable names, and every set the profile holds.
    const names = new Set(a.variables.map((v) => v.valueLabels).filter(Boolean));
    for (const name of Object.keys(f.expected.dta?.value_labels ?? {})) names.add(name);
    names.add("no such set");
    for (const name of names) assert.deepEqual(plain([...(a.valueLabels(name) ?? [])]), plain([...(b.valueLabels(name) ?? [])]), `labels ${name}`);

    if (usesBrotli(b)) {
      // Dropped on purpose: every read names the codec.
      if (readable.length) assert.throws(() => a.readColumns([readable[0]]), (e) => e.code === "DTA_INVALID" && /brotli compression is not supported/.test(e.message));
      return;
    }
    const nobs = a.info.nobs;
    for (const [start, count] of windows(nobs, a.info.rowGroups)) {
      const got = a.readColumns(readable, { start, count });
      const want = b.readColumns(readable, { start, count });
      readable.forEach((k, j) => {
        assert.equal(got[j].kind, want[j].kind, `${a.variables[k].name} kind`);
        assert.deepEqual(rowsOf(got[j]), rowsOf(want[j]), `${a.variables[k].name} rows ${start} to ${start + count}`);
        if (want[j].kind === "dictionary") {
          assert.deepEqual([...got[j].codes], [...want[j].codes], `${a.variables[k].name} codes`);
          assert.deepEqual(got[j].values, want[j].values, `${a.variables[k].name} levels`);
        }
        if (want[j].kind === "bigint") assert.equal(got[j].values.constructor, want[j].values.constructor);
      });
    }
    // A variable asked for twice, in another order.
    if (readable.length > 1) {
      const cols = [readable[1], readable[0], readable[1]];
      assert.deepEqual(a.readColumns(cols).map(rowsOf), b.readColumns(cols).map(rowsOf), "repeated columns");
    }
    const rows = scattered(nobs);
    assert.deepEqual(plain(a.readRowsAt(rows, readable)), plain(b.readRowsAt(rows, readable)), "readRowsAt");
    a.close();
    b.close();
  });
}

for (const f of fixtures()) {
  test(`${f.set}/${f.name}: int64 "bigint" and "number" as 1.1.0 reads them`, () => {
    const bytes = bytesOf(f.path);
    for (const int64 of ["bigint", "number"]) {
      const a = pq.openParquet(bytes, { ...OPTIONS, int64 });
      const b = old.openParquet(bytes, { ...OPTIONS, int64 });
      const readable = [];
      a.variables.forEach((v, k) => {
        if (wideDecimal(b.variables[k])) return;
        assert.deepEqual(variableOf(v), variableOf(b.variables[k]), `${int64}: ${v.name}`);
        if (v.columnKind !== null) readable.push(k);
      });
      if (usesBrotli(b)) continue;
      for (const k of readable) {
        let want;
        try {
          want = rowsOf(b.readColumns([k])[0]);
        } catch (e) {
          // A value beyond the safe integers, read as a number.
          assert.throws(() => a.readColumns([k]), (x) => x.code === e.code && /beyond the safe integers/.test(x.message), `${int64}: ${a.variables[k].name}`);
          continue;
        }
        assert.deepEqual(rowsOf(a.readColumns([k])[0]), want, `${int64}: ${a.variables[k].name}`);
      }
    }
  });
}
