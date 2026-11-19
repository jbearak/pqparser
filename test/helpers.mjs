// Shared by the tests: both readers, the fixtures, and values made comparable.
import { readFileSync, readdirSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";

const require = createRequire(import.meta.url);
export const pq = require("../dist/index.js");
export const pqNode = require("../dist/node.js");
/** @jbearak/dta-parser 1.1.0, the reader pqparser replaces. */
export const old = require("@jbearak/dta-parser");
export const oldNode = require("@jbearak/dta-parser/node");

/** What table-viewer opens every Parquet file with. */
export const OPTIONS = Object.freeze({ nested: "error", int64: "auto", pageCacheBytes: 32 * 1024 * 1024 });

export const FIXTURES = new URL("./fixtures/", import.meta.url).pathname;

/** Every fixture: `{ name, set, path, text, expected }`: set "compat" or "legacy", pyarrow's JSON as text and parsed. */
export function fixtures() {
  const out = [];
  for (const set of ["compat", "legacy"]) {
    const dir = join(FIXTURES, set);
    for (const file of readdirSync(dir).filter((f) => f.endsWith(".parquet")).sort()) {
      const name = file.slice(0, -".parquet".length);
      const text = readFileSync(join(dir, "expected", `${name}.json`), "utf8");
      out.push({ name, set, path: join(dir, file), text, expected: JSON.parse(text) });
    }
  }
  return out;
}

/** A value made comparable with `assert.deepStrictEqual`, bigints and bytes included. */
export function plain(v) {
  if (typeof v === "bigint") return `${v}n`;
  if (v instanceof Uint8Array) return `bytes:${Buffer.from(v).toString("hex")}`;
  if (typeof v === "number") return Object.is(v, -0) ? "-0" : Number.isNaN(v) ? "NaN" : v;
  if (v && typeof v === "object" && "missing" in v && Object.keys(v).length === 1) return `missing:${v.missing}`;
  if (Array.isArray(v)) return v.map(plain);
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, plain(x)]));
  return v;
}

/** A column's rows, each comparable: a number, a missing code, text, bytes or a bigint. */
export function rowsOf(c) {
  const n = c.kind === "dictionary" ? c.codes.length : c.values.length;
  const out = new Array(n);
  for (let i = 0; i < n; i++) {
    switch (c.kind) {
      case "number":
        out[i] = c.missing[i] ? `missing:${c.missing[i]}` : plain(c.values[i]);
        break;
      case "bigint":
        out[i] = c.missing[i] ? "null" : plain(c.values[i]);
        break;
      case "dictionary":
        out[i] = c.missing?.[i] ? "null" : c.values[c.codes[i]];
        break;
      default:
        out[i] = c.missing?.[i] ? `null:${plain(c.values[i])}` : plain(c.values[i]);
    }
  }
  return out;
}

/** A variable as plain data, its chunks' encodings sorted (arrow-rs keeps them as a set). */
export function variableOf(v, { sortEncodings = true } = {}) {
  const out = plain({ ...v, chunks: v.chunks.map((c) => (c && sortEncodings ? { ...c, encodings: [...c.encodings].sort() } : c)) });
  return out;
}

export function bytesOf(path) {
  return readFileSync(path);
}

// pyarrow's values (scripts/expected.py) against ours, by the rules of
// dta-parser's crates/parquet/tests/compat.rs (see pyarrow.test.mjs).

const UNITS = ["s", "ms", "us", "ns"];

/** Parse JSON keeping integers beyond the safe ones exact, as bigints. */
export function parseExact(text) {
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
export function cell(c, i) {
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
export function same(want, got, type, variable) {
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
  // A DECIMAL of more than 15 digits is text, with every digit.
  if (/^decimal/.test(type)) return typeof got === "string" ? got === want : got === Number(want);
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
