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

/** Fixtures 1.1.0 reads with a codec pqparser drops on purpose. */
export function usesBrotli(reader) {
  return reader.info.compression.includes("brotli");
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
