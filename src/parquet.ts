// ParquetReader: @jbearak/dta-parser 1.1.0's Parquet reading API (the
// part table-viewer uses), over arrow-rs. The module (src/lib.rs) gives
// the footer's facts and decoded columns; what they mean here (types,
// the dta profile, `.a` to `.z`, 64-bit integers) follows 1.1.0.

import { Core } from "./wasm";
import { dtaError } from "./errors";
import { MISSING, missingValue, type Cell, type MissingName, type ValueLabels } from "./values";

/** Reads `into.length` bytes of the file from `offset` into `into`, or throws. */
export type ReadAt = (into: Uint8Array, offset: number) => void;

/** Options for opening a Parquet file. */
export interface ParquetOptions {
  /**
   * Nested columns (lists, maps, structs) are not read: they are in
   * `variables` with role `"nested"`, and reading them, or reading every
   * column, is an error naming them. Only `"error"` is accepted.
   */
  nested?: "error";
  /**
   * How 64-bit integer columns (INT64, unsigned 64-bit, and timestamps
   * and times stored as INT64 or INT96) come back. `"auto"` (the
   * default): as a `NumberColumn` when the footer's statistics, present
   * for every row group, put every value within the safe integers;
   * otherwise as a `BigIntColumn`. `ParquetVariable.int64` says which and
   * why. A value read as a number beyond the safe integers is an error.
   * `"bigint"` or `"number"` choose for every such column.
   */
  int64?: "auto" | "bigint" | "number";
  /** Accepted and ignored: arrow-rs reads only the pages a read needs, and keeps none. */
  pageCacheBytes?: number;
}

/** One variable's descriptor. */
export interface Variable {
  name: string;
  type: string;
  /** Display format, such as `%9.0g`, from the dta profile. */
  format: string;
  /** Variable label; empty when there is none. */
  label: string;
  /** Name of the value label set attached; empty when there is none. */
  valueLabels: string;
}

/** What every reader's `info` says. */
export interface TableInfo {
  format: "dta" | "parquet";
  nobs: number;
  label: string;
  sortedBy: string[];
}

/** A row group, as `ParquetInfo.rowGroups` lists them. */
export interface RowGroup {
  /** Its first row in the file, counted from 0. */
  firstRow: number;
  rows: number;
  compressedBytes: number;
  uncompressedBytes: number;
}

/** A statistic from the footer: a number, a bigint (by the `int64` rule), text, or bytes. */
export type Statistic = number | bigint | string | Uint8Array;

/** One column's part of one row group (a column chunk), from the footer. */
export interface ColumnChunk {
  compressedBytes: number;
  uncompressedBytes: number;
  /** The codec: `"snappy"`, `"zstd"`, `"gzip"`, `"lz4_raw"`, `"brotli"` or `"none"`. */
  codec: string;
  /** The encodings it uses, such as `"PLAIN"` and `"RLE_DICTIONARY"` (in the order of Parquet's enum). */
  encodings: string[];
  /** Bytes of its dictionary page, header included; null without one. */
  dictionaryPageBytes: number | null;
  /** Nulls in it, as the writer counted them; null when the file does not say. */
  nullCount: number | null;
  /** The least and greatest value, as the writer recorded them; null when the file does not say (always for DECIMAL and INT96). */
  min: Statistic | null;
  max: Statistic | null;
}

/** What a Parquet file's footer says about it. */
export interface ParquetInfo extends TableInfo {
  format: "parquet";
  rowGroups: RowGroup[];
  /** The codecs the column chunks use, each once. */
  compression: string[];
  createdBy: string;
  /** The dataset's notes, from the dta profile. */
  notes: string[];
  /** Bytes of the footer. */
  footerBytes: number;
  /** Each key-value metadata entry's key and size in bytes. */
  keyValue: { key: string; bytes: number }[];
  /** Warnings about a damaged or newer dta profile, or an unreadable `ARROW:schema`. */
  warnings: string[];
}

/** One column of a Parquet file, as @jbearak/dta-parser 1.1.0 describes it. */
export interface ParquetVariable extends Variable {
  role: "data" | "nested";
  physicalType: string;
  logicalType: string;
  columnKind: "number" | "bigint" | "string" | "dictionary" | null;
  notRead?: string;
  int64?: { kind: "number" | "bigint"; reason: string };
  unit?: "day" | "s" | "ms" | "us" | "ns";
  timeZone?: string;
  storage?: string;
  categorical?: { levels: string[] | null; ordered: boolean };
  readonly chunks: (ColumnChunk | null)[];
}

/** Numbers: where `missing[i]` is not 0, `values[i]` is NaN: 1 is `.`, 2 to 27 `.a` to `.z`. */
export interface NumberColumn {
  kind: "number";
  values: Float64Array;
  missing: Uint8Array;
}

/** Text, or bytes (Uint8Arrays); `missing[i]` is 1 for a null, whose value is empty. */
export interface StringColumn {
  kind: "string";
  values: (string | Uint8Array)[];
  missing?: Uint8Array;
}

/** 64-bit integers; where `missing[i]` is 1 the value is null and `values[i]` is 0. */
export interface BigIntColumn {
  kind: "bigint";
  values: BigInt64Array | BigUint64Array;
  missing: Uint8Array;
}

/** A categorical: row `i` is `values[codes[i]]`, unless `missing[i]` is 1. */
export interface DictionaryColumn {
  kind: "dictionary";
  codes: Uint32Array;
  values: string[];
  missing?: Uint8Array;
}

export type Column = NumberColumn | StringColumn;
export type TableColumn = Column | DictionaryColumn | BigIntColumn;
export type TableCell = Cell | bigint;

/** Which rows `readColumns` reads. */
export interface ReadRange {
  /** First row; default 0. */
  start?: number;
  /** How many rows; default all from `start`. */
  count?: number;
}

/** What `ParquetReader` shares with @jbearak/dta-parser's readers. */
export interface TableReader {
  readonly info: TableInfo;
  readonly variables: readonly Variable[];
  readColumns(columns: readonly (number | string)[], range?: ReadRange): TableColumn[];
  readRows(start: number, count: number, columns?: readonly (number | string)[]): TableCell[][];
  readRowsAt(rows: readonly number[], columns?: readonly (number | string)[]): TableCell[][];
  valueLabels(name: string): ValueLabels | undefined;
  close(): void;
  [Symbol.dispose](): void;
}

// ---------------------------------------------------------------- the footer

/** The module's JSON (src/lib.rs, `pq_meta`). */
type RawLogical = [string, ...(number | string | boolean)[]];
type RawField = [string, boolean, number, RawLogical | null, string, string?, number?, number?, number?];
type RawChunk = [string, string[], number, number, number | null, number, number | null, number | string | null, number | string | null, boolean];
interface RawMeta {
  createdBy: string;
  keyValue: [string, number][];
  arrow: null | string | [string, boolean, string | null][];
  fields: RawField[];
  rowGroups: [number, RawChunk[]][];
}

/** What a column holds, from its Parquet types (1.1.0's `ColumnType`). */
type Kind =
  | { t: "int8" | "int16" | "int32" | "int64" | "uint8" | "uint16" | "uint32" | "uint64" | "float" | "double" | "bool" | "text" | "binary" | "date" }
  | { t: "timestamp" | "time"; unit: "ms" | "us" | "ns"; utc: boolean }
  | { t: "decimal"; precision: number; scale: number }
  | { t: "unsupported"; why: string };

const UNITS: Record<string, "ms" | "us" | "ns"> = { MILLIS: "ms", MICROS: "us", NANOS: "ns" };
const CONVERTED: Record<string, RawLogical> = {
  UTF8: ["STRING"],
  ENUM: ["ENUM"],
  JSON: ["JSON"],
  BSON: ["BSON"],
  DATE: ["DATE"],
  TIME_MILLIS: ["TIME", "MILLIS", true],
  TIME_MICROS: ["TIME", "MICROS", true],
  TIMESTAMP_MILLIS: ["TIMESTAMP", "MILLIS", true],
  TIMESTAMP_MICROS: ["TIMESTAMP", "MICROS", true],
  INT_8: ["INT", 8, true],
  INT_16: ["INT", 16, true],
  INT_32: ["INT", 32, true],
  INT_64: ["INT", 64, true],
  UINT_8: ["INT", 8, false],
  UINT_16: ["INT", 16, false],
  UINT_32: ["INT", 32, false],
  UINT_64: ["INT", 64, false],
};

/** Parquet's logical type (or else its converted type) as 1.1.0 shows it. */
function logicalName(l: RawLogical | null, converted: string): string {
  if (!l) return /^(MAP|MAP_KEY_VALUE|LIST|NONE)$/.test(converted) ? "" : converted;
  switch (l[0]) {
    case "DECIMAL":
      return `DECIMAL(${l[1]}, ${l[2]})`;
    case "TIME":
    case "TIMESTAMP":
      return `${l[0]}(${l[1]}, ${l[2] ? "UTC" : "local"})`;
    case "INT":
      return `INT(${l[1]}, ${l[2] ? "signed" : "unsigned"})`;
    case "UNKNOWN":
      return "NULL";
    default:
      return l[0];
  }
}

/** A leaf's kind from its physical and logical types (1.1.0's `column_type`). */
function kindOf(f: RawField): Kind {
  const [, , , rawLogical, converted, physical, length, precision, scale] = f;
  if (converted === "INTERVAL" && !rawLogical) return { t: "unsupported", why: "an INTERVAL, which this version does not read" };
  let l = rawLogical && rawLogical[0] !== "UNKNOWN" ? rawLogical : null;
  if (!l && converted === "DECIMAL") l = ["DECIMAL", precision ?? 0, scale ?? 0];
  if (!l) l = CONVERTED[converted] ?? null;
  const name = l?.[0];
  if (physical === "BOOLEAN") return { t: "bool" };
  if (physical === "INT96") return { t: "timestamp", unit: "ns", utc: false };
  if (physical === "FLOAT") return { t: "float" };
  if (physical === "DOUBLE") return { t: "double" };
  if (name === "INT" && (physical === "INT32" || physical === "INT64")) {
    const t = `${l![2] ? "int" : "uint"}${l![1]}`;
    if (physical === "INT64" ? l![1] === 64 : l![1] !== 64) return { t: t as "int8" };
  } else if (name === "DECIMAL") {
    const [p, s] = [l![1] as number, l![2] as number];
    if (p < 1 || p > 76 || s < 0 || s > p || (physical === "FIXED_LEN_BYTE_ARRAY" && !(length! > 0 && length! <= 32))) {
      return { t: "unsupported", why: `DECIMAL(${p}, ${s}), which is not valid` };
    }
    return { t: "decimal", precision: p, scale: s };
  } else if (name === "DATE" && physical === "INT32") return { t: "date" };
  else if ((name === "TIME" || name === "TIMESTAMP") && (physical === "INT32" || physical === "INT64")) {
    return { t: name === "TIME" ? "time" : "timestamp", unit: UNITS[l![1] as string], utc: l![2] as boolean };
  } else if (name === "FLOAT16") return { t: "unsupported", why: "FLOAT16, which is not read" };
  else if (physical === "BYTE_ARRAY" && (name === "STRING" || name === "ENUM" || name === "JSON")) return { t: "text" };
  else if (physical === "BYTE_ARRAY" && (name === "BSON" || !l)) return { t: "binary" };
  else if (physical === "FIXED_LEN_BYTE_ARRAY" && (!l || name === "UUID")) return { t: "binary" };
  else if (!l && physical === "INT32") return { t: "int32" };
  else if (!l && physical === "INT64") return { t: "int64" };
  return { t: "unsupported", why: `${physical} annotated ${logicalName(l, converted)}, which this version does not read` };
}

// ---------------------------------------------------------------- the dta profile

interface ColumnProfile {
  label?: string;
  format?: string;
  value_labels?: string;
  storage?: string;
  logical?: string;
  categorical?: { levels: string[]; ordered: boolean };
  tz?: string;
}

interface Profile {
  label?: string;
  notes: string[];
  sort: string[];
  columns: Map<string, ColumnProfile>;
  /** Each value label set's JSON, parsed when first wanted. */
  sets: Map<string, unknown>;
}

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const isStrings = (v: unknown): v is string[] => Array.isArray(v) && v.every((s) => typeof s === "string");
const dropped = (what: string, why: string) => `dropped "${what}" from the dta profile: it ${why}`;

/** The dta profile (docs/parquet.md in dta-parser), forgiving as 1.1.0 is: damage is a warning. */
function parseProfile(text: string, warn: (w: string) => void): Profile | undefined {
  let v: unknown;
  try {
    v = JSON.parse(text);
  } catch (e) {
    warn(`the dta profile is not JSON (${(e as Error).message}); the file is read as plain Parquet`);
    return undefined;
  }
  if (!isObject(v)) {
    warn(`the dta profile is ${Array.isArray(v) ? "an array" : v === null ? "null" : typeof v}, not an object; the file is read as plain Parquet`);
    return undefined;
  }
  const p: Profile = { notes: [], sort: [], columns: new Map(), sets: new Map() };
  const version = v.version;
  if (version === undefined) warn('the dta profile has no "version"; it is read as version 1');
  else if (!(Number.isInteger(version) && (version as number) >= 1)) warn(`dropped the dta profile's "version": it is ${typeof version}, not a positive integer`);
  else if ((version as number) > 1) warn(`the dta profile is version ${version}, newer than this reader's 1; what this reader knows is used`);
  if (v.label !== undefined) typeof v.label === "string" ? (p.label = v.label) : warn(dropped("label", "is not text"));
  for (const key of ["notes", "sort"] as const) {
    if (v[key] !== undefined) isStrings(v[key]) ? (p[key] = v[key]) : warn(dropped(key, "is not an array of text"));
  }
  if (isObject(v.value_labels)) for (const [name, set] of Object.entries(v.value_labels)) p.sets.set(name, set);
  else if (v.value_labels !== undefined) warn(dropped("value_labels", "is not an object"));
  if (isObject(v.columns)) {
    for (const [name, entry] of Object.entries(v.columns)) {
      if (!isObject(entry)) {
        warn(dropped(`columns.${name}`, "is not an object"));
        continue;
      }
      const c: ColumnProfile = {};
      for (const key of ["label", "format", "value_labels", "tz", "unit"] as const) {
        if (entry[key] === undefined) continue;
        if (typeof entry[key] === "string") (c as Record<string, string>)[key] = entry[key];
        else warn(dropped(`columns.${name}.${key}`, "is not text"));
      }
      if (entry.storage !== undefined) {
        if (["byte", "int", "long", "float"].includes(entry.storage as string)) c.storage = entry.storage as string;
        else warn(dropped(`columns.${name}.storage`, 'is not one of "byte", "int", "long" and "float"'));
      }
      if (entry.logical !== undefined) {
        if (entry.logical === "date" || entry.logical === "timestamp") c.logical = entry.logical;
        else warn(dropped(`columns.${name}.logical`, 'is not one of "date" and "timestamp"'));
      }
      const cat = entry.categorical;
      if (cat !== undefined) {
        const why = !isObject(cat)
          ? "is not an object"
          : !isStrings(cat.levels)
            ? "has levels that are not an array of text"
            : new Set(cat.levels).size < cat.levels.length
              ? "has a level more than once"
              : cat.ordered !== undefined && typeof cat.ordered !== "boolean"
                ? 'has an "ordered" that is not true or false'
                : undefined;
        if (why) warn(dropped(`columns.${name}.categorical`, why));
        else c.categorical = { levels: (cat as { levels: string[] }).levels, ordered: (cat as { ordered?: boolean }).ordered ?? false };
      }
      if (c.value_labels !== undefined && !p.sets.has(c.value_labels)) {
        warn(dropped(`columns.${name}.value_labels`, `names the set "${c.value_labels}", which the profile does not have`));
        delete c.value_labels;
      }
      p.columns.set(name, c);
    }
  } else if (v.columns !== undefined) warn(dropped("columns", "is not an object"));
  return p;
}

const MAX_EXACT = 2 ** 53;

/** A value label set from its JSON, or undefined when it breaks a rule (1.1.0 leaves such a set out). */
function labelSet(v: unknown): ValueLabels | undefined {
  if (!Array.isArray(v)) return undefined;
  const map: ValueLabels = new Map();
  for (const pair of v) {
    if (!Array.isArray(pair) || pair.length !== 2 || typeof pair[1] !== "string") return undefined;
    const [code, text] = pair;
    let key: number | Cell;
    if (typeof code === "number") {
      if (!Number.isFinite(code) || (Number.isInteger(code) && Math.abs(code) > MAX_EXACT)) return undefined;
      key = code;
    } else if (typeof code === "string" && code in MISSING) key = MISSING[code];
    else return undefined;
    if (map.has(key as number)) return undefined;
    map.set(key as number, text);
  }
  return map;
}

// ---------------------------------------------------------------- variables

const SAFE = BigInt(Number.MAX_SAFE_INTEGER);
/** How a column reads in the module (src/lib.rs). */
const NUMBER = 0;
const BIGINT = 1;
const TEXT = 2;
const BYTES = 3;
/** Codecs the module reads. */
const CODECS = new Set(["none", "snappy", "gzip", "zstd", "lz4_raw"]);

const utf8 = new TextDecoder("utf-8", { ignoreBOM: true });
const strictUtf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

function fromHex(h: string): Uint8Array {
  return Uint8Array.from(h.match(/../g) ?? [], (x) => parseInt(x, 16));
}

/** A variable from the footer (1.1.0's `variable_json` and `chunks_json`). */
function describe(
  f: RawField,
  kind: Kind,
  meta: RawMeta,
  int64: string,
  profile: Profile | undefined,
  extra: { storage?: string; logical?: string; levels?: "profile" | "rows"; zone?: string | null },
): { v: ParquetVariable; mode: number } {
  const [name, nested, leaf, rawLogical, converted, physical] = f;
  const entry = profile?.columns.get(name);
  const isInt64 = (physical === "INT64" || physical === "INT96") && ["int64", "uint64", "timestamp", "time"].includes(kind.t);
  let columnKind: ParquetVariable["columnKind"] = null;
  let notRead: string | undefined;
  let mode = NUMBER;
  let int64Info: ParquetVariable["int64"];
  if (nested) notRead = `column "${name}" is nested (a list, map or group), which is not read`;
  else if (kind.t === "unsupported") notRead = `column "${name}" is ${kind.why}`;
  else if (kind.t === "text") [columnKind, mode] = extra.levels ? ["dictionary", TEXT] : ["string", TEXT];
  else if (kind.t === "binary") [columnKind, mode] = ["string", BYTES];
  else if (kind.t === "decimal" && kind.precision > 15) {
    notRead = `column "${name}" is DECIMAL(${kind.precision}, ${kind.scale}), with more digits than a number holds, which is not read`;
  } else if (isInt64) {
    const [number, reason] =
      int64 === "bigint"
        ? [false, 'int64: "bigint" was asked for']
        : int64 === "number"
          ? [true, 'int64: "number" was asked for']
          : int64Auto(meta, leaf, physical!, kind.t === "uint64");
    int64Info = { kind: number ? "number" : "bigint", reason };
    [columnKind, mode] = number ? ["number", NUMBER] : ["bigint", BIGINT];
  } else columnKind = "number";

  const type = nested
    ? "nested"
    : kind.t === "double"
      ? (extra.logical ?? extra.storage ?? "double")
      : kind.t === "text"
        ? extra.levels
          ? "categorical"
          : "string"
        : ({ int8: "byte", int16: "int", int32: "long", float: "float", bool: "bool", unsupported: "unsupported" } as Record<string, string>)[kind.t] ?? kind.t;
  const labelsApply =
    (columnKind === "number" || columnKind === "bigint") && !["date", "timestamp", "time", "bool"].includes(kind.t) && !extra.logical;
  const v: Record<string, unknown> = {
    name,
    type,
    format: entry?.format ?? "",
    label: entry?.label ?? "",
    valueLabels: entry?.value_labels !== undefined && labelsApply ? entry.value_labels : "",
    role: nested ? "nested" : "data",
    physicalType: physical ?? "",
    logicalType: logicalName(rawLogical, converted),
    columnKind,
  };
  if (notRead) v.notRead = notRead;
  if (int64Info) v.int64 = int64Info;
  const unit = kind.t === "date" ? "day" : "unit" in kind ? kind.unit : kind.t === "double" ? { date: "day", timestamp: "s" }[extra.logical ?? ""] : undefined;
  if (unit) v.unit = unit;
  const instant = (kind.t === "timestamp" && kind.utc) || (kind.t === "double" && extra.logical === "timestamp");
  if (instant) v.timeZone = entry?.tz || extra.zone || "UTC";
  if (extra.storage) v.storage = extra.storage;
  if (columnKind === "dictionary") {
    const cat = extra.levels === "profile" ? entry?.categorical : undefined;
    v.categorical = Object.freeze({ levels: cat ? cat.levels : null, ordered: cat?.ordered ?? false });
  }
  v.chunks = Object.freeze(
    nested
      ? []
      : meta.rowGroups.map(([, chunks]) => {
          const c = chunks[leaf];
          return Object.freeze(chunkOf(c, kind, physical!, columnKind));
        }),
  );
  return { v: Object.freeze(v) as unknown as ParquetVariable, mode };
}

/** How `int64: "auto"` reads a 64-bit column, and why (1.1.0's `int64_auto`). */
function int64Auto(meta: RawMeta, leaf: number, physical: string, unsigned: boolean): [boolean, string] {
  if (physical === "INT96") return [false, "INT96 timestamps have no statistics to show their range"];
  for (let g = 0; g < meta.rowGroups.length; g++) {
    const [, , , , , values, nulls, min, max, deprecated] = meta.rowGroups[g][1][leaf];
    if (nulls === null && min === null && max === null) return [false, `row group ${g + 1} has no statistics`];
    if (nulls !== null && nulls === values) continue;
    if (min === null || max === null || (unsigned && deprecated)) return [false, `row group ${g + 1} has no minimum and maximum`];
    const as = (x: number | string) => (unsigned ? BigInt.asUintN(64, BigInt(x)) : BigInt(x));
    const [lo, hi] = [as(min), as(max)];
    if (lo < -SAFE || hi > SAFE) {
      return [false, `row group ${g + 1}'s statistics hold ${hi > SAFE ? hi : lo}, beyond the safe integers (±(2^53 − 1))`];
    }
  }
  return [true, "the statistics of every row group put every value within the safe integers (±(2^53 − 1))"];
}

/** Kinds whose deprecated min and max are in the right (signed) order. */
const OLD_OK = new Set(["int8", "int16", "int32", "int64", "float", "double", "date", "timestamp"]);

function chunkOf(c: RawChunk, kind: Kind, physical: string, columnKind: ParquetVariable["columnKind"]): ColumnChunk {
  const [codec, encodings, compressedBytes, uncompressedBytes, dictionaryPageBytes, , nullCount, min, max, deprecated] = c;
  const stat = (s: number | string | null): Statistic | null => {
    if (s === null || kind.t === "decimal" || physical === "INT96" || (deprecated && !OLD_OK.has(kind.t))) return null;
    switch (physical) {
      case "INT32":
        return ["uint8", "uint16", "uint32"].includes(kind.t) ? (s as number) >>> 0 : (s as number);
      case "INT64": {
        const b = kind.t === "uint64" ? BigInt.asUintN(64, BigInt(s)) : BigInt(s);
        return columnKind === "bigint" ? b : Number(b);
      }
      case "BYTE_ARRAY":
      case "FIXED_LEN_BYTE_ARRAY": {
        const bytes = fromHex(s as string);
        if (physical === "BYTE_ARRAY" && kind.t === "text") {
          try {
            return strictUtf8.decode(bytes);
          } catch {
            return bytes;
          }
        }
        return bytes;
      }
      default:
        return s as number;
    }
  };
  return { compressedBytes, uncompressedBytes, codec, encodings, dictionaryPageBytes, nullCount, min: stat(min), max: stat(max) };
}

// ---------------------------------------------------------------- the reader

/** A reader dropped without `close()` still closes the file it opened. */
const unclosed = new FinalizationRegistry((close: () => void) => close());

/**
 * An open Parquet file, read synchronously; open one with `openParquet`
 * or `openParquetFile` (`pqparser/node`). Opening reads only the footer.
 */
export class ParquetReader implements TableReader {
  readonly info: ParquetInfo;
  readonly variables: readonly ParquetVariable[];
  private core: Core | undefined;
  /** How the module reads each variable (src/lib.rs's modes). */
  private readonly modes: number[];
  private readonly profile?: Profile;
  private readonly labels = new Map<string, ValueLabels | undefined>();
  private readonly nested: string[];
  private readonly onClose?: () => void;

  /** Open a file given a function that reads its bytes and its length. */
  constructor(read: ReadAt, length: number, options: ParquetOptions = {}, onClose?: () => void) {
    const int64 = options.int64 ?? "auto";
    if (!["auto", "bigint", "number"].includes(int64)) {
      throw dtaError("DTA_INVALID", `int64 must be "auto", "bigint" or "number", not ${JSON.stringify(int64)}`, undefined, TypeError);
    }
    const nested = options.nested ?? "error";
    if (nested !== "error") throw dtaError("DTA_INVALID", `nested must be "error" (pqparser does not skip nested columns), not ${JSON.stringify(nested)}`, undefined, TypeError);
    const cache = options.pageCacheBytes ?? 0;
    if (!Number.isFinite(cache) || cache < 0) throw dtaError("DTA_INVALID", `pageCacheBytes must be 0 or more, not ${cache}`, undefined, RangeError);
    const core = new Core(read);
    core.call((x) => x.pq_open(length));
    core.call((x) => x.pq_meta());
    const meta: RawMeta = JSON.parse(core.text());
    const tail = new Uint8Array(8);
    read(tail, length - 8);
    const warnings: string[] = [];
    const warn = (w: string) => warnings.push(w);
    const kvIndex = (key: string) => meta.keyValue.findIndex(([k]) => k === key);
    let profile: Profile | undefined;
    if (kvIndex("dta") >= 0) {
      core.call((x) => x.pq_key_value(kvIndex("dta")));
      profile = parseProfile(core.text(), warn);
    }
    // What the profile and ARROW:schema add to each column (1.1.0's
    // `check_widened`, levels from the profile, and `apply_arrow`).
    const kinds = meta.fields.map((f) => (f[1] ? ({ t: "unsupported", why: "nested" } as Kind) : kindOf(f)));
    const firstData = new Map<string, number>();
    const first = new Map<string, number>();
    meta.fields.forEach((f, k) => {
      if (!first.has(f[0])) first.set(f[0], k);
      if (!f[1] && !firstData.has(f[0])) firstData.set(f[0], k);
    });
    const extra = meta.fields.map(() => ({}) as { storage?: string; logical?: string; levels?: "profile" | "rows"; zone?: string | null });
    for (const [name, c] of profile?.columns ?? []) {
      if (c.storage !== undefined || c.logical !== undefined) {
        const k = firstData.get(name);
        if (k !== undefined && kinds[k].t === "double") Object.assign(extra[k], { storage: c.storage, logical: c.logical });
        else if (k !== undefined) {
          if (c.storage !== undefined) warn(`dropped "columns.${name}.storage" from the dta profile: it gives a storage type to a column that is not DOUBLE`);
          if (c.logical !== undefined) warn(`dropped "columns.${name}.logical" from the dta profile: it gives a date or timestamp type to a column that is not DOUBLE`);
        }
      }
      const k = first.get(name);
      if (c.categorical && k !== undefined) {
        if (kinds[k].t !== "text") warn(`the dta profile gives levels for column "${name}", which is not text; they are ignored`);
        else extra[k].levels = "profile";
      }
    }
    if (typeof meta.arrow === "string") warn(`the file's ARROW:schema entry could not be read (${meta.arrow}); it is ignored`);
    else if (meta.arrow) {
      const byName = new Map<string, [string, boolean, string | null]>();
      for (const a of meta.arrow) if (!byName.has(a[0])) byName.set(a[0], a);
      meta.fields.forEach((f, k) => {
        const a = byName.get(f[0]);
        if (f[1] || !a) return;
        if (a[1] && kinds[k].t === "text" && !extra[k].levels) extra[k].levels = "rows";
        if (kinds[k].t === "timestamp") extra[k].zone = a[2];
      });
    }
    const described = meta.fields.map((f, k) => describe(f, kinds[k], meta, int64, profile, extra[k]));
    this.variables = Object.freeze(described.map((d) => d.v));
    this.modes = described.map((d) => d.mode);
    this.nested = this.variables.filter((v) => v.role === "nested").map((v) => v.name);
    this.profile = profile;
    this.core = core;
    this.onClose = onClose;
    if (onClose) unclosed.register(this, onClose, this);
    const compression: string[] = [];
    let firstRow = 0;
    const rowGroups = meta.rowGroups.map(([rows, chunks]) => {
      let [compressedBytes, uncompressedBytes] = [0, 0];
      for (const c of chunks) {
        compressedBytes += c[2];
        uncompressedBytes += c[3];
        if (!compression.includes(c[0])) compression.push(c[0]);
      }
      const g = Object.freeze({ firstRow, rows, compressedBytes, uncompressedBytes });
      firstRow += rows;
      return g;
    });
    this.info = Object.freeze({
      format: "parquet" as const,
      nobs: firstRow,
      label: profile?.label ?? "",
      sortedBy: profile?.sort ?? [],
      notes: profile?.notes ?? [],
      rowGroups,
      compression,
      createdBy: meta.createdBy,
      footerBytes: new DataView(tail.buffer).getUint32(0, true),
      keyValue: meta.keyValue.map(([key, bytes]) => ({ key, bytes })),
      warnings,
    });
  }

  private get open(): Core {
    if (!this.core) throw dtaError("DTA_CLOSED", "the file is closed");
    return this.core;
  }

  /** The value label set `name` from the dta profile, or undefined (also for a set that breaks a rule). */
  valueLabels(name: string): ValueLabels | undefined {
    this.open;
    if (!this.labels.has(name)) this.labels.set(name, this.profile?.sets.has(name) ? labelSet(this.profile.sets.get(name)) : undefined);
    return this.labels.get(name);
  }

  private index(column: number | string): number {
    const i = typeof column === "number" ? column : this.variables.findIndex((v) => v.name === column);
    if (!Number.isInteger(i) || i < 0 || i >= this.variables.length) {
      const names = this.variables.slice(0, 20).map((v) => JSON.stringify(v.name)).join(", ");
      const more = this.variables.length > 20 ? ` and ${this.variables.length - 20} more` : "";
      throw dtaError("DTA_OUT_OF_RANGE", `no variable ${JSON.stringify(column)}; the file's are ${names}${more}`, undefined, RangeError);
    }
    return i;
  }

  /** Every variable, or the error a read of every variable gives. */
  private all(): number[] {
    if (this.nested.length) {
      throw dtaError(
        "DTA_INVALID",
        `the file has nested columns, which are not read: ${this.nested.map((n) => JSON.stringify(n)).join(", ")}; select the columns wanted`,
        undefined,
        TypeError,
      );
    }
    return this.variables.map((_, i) => i);
  }

  private rows(start: number, count: number): void {
    if (!Number.isInteger(start) || !Number.isInteger(count) || start < 0 || count < 0 || start + count > this.info.nobs) {
      throw dtaError("DTA_OUT_OF_RANGE", `rows ${start} to ${start + count} are outside 0 to ${this.info.nobs}`, undefined, RangeError);
    }
  }

  /** Read `count` rows from row `start` (default: all rows) of the given variables, by index or name, in the order given. */
  readColumns(columns: readonly (number | string)[], range: ReadRange = {}): TableColumn[] {
    const idx = columns.map((c) => this.index(c));
    const start = range.start ?? 0;
    const count = range.count ?? this.info.nobs - start;
    this.rows(start, count);
    return this.gather(idx, start, count);
  }

  /** Rows as arrays of cells, one per variable asked for (default: all); a null is `MISSING["."]`. */
  readRows(start: number, count: number, columns?: readonly (number | string)[]): TableCell[][] {
    const idx = columns ? columns.map((c) => this.index(c)) : this.all();
    this.rows(start, count);
    const cols = this.gather(idx, start, count);
    return Array.from({ length: count }, (_, i) => cols.map((c) => cellOf(c, i)));
  }

  /** The rows listed, in their order (repeats allowed), as `readRows` gives them. Nearby rows are read together. */
  readRowsAt(rows: readonly number[], columns?: readonly (number | string)[]): TableCell[][] {
    const idx = columns ? columns.map((c) => this.index(c)) : this.all();
    for (const r of rows) {
      if (!Number.isInteger(r) || r < 0 || r >= this.info.nobs) {
        throw dtaError("DTA_OUT_OF_RANGE", `row ${r} is outside 0 to ${this.info.nobs - 1}`, undefined, RangeError);
      }
    }
    const spans: { start: number; count: number }[] = [];
    for (const r of Float64Array.from(new Set(rows)).sort()) {
      const last = spans[spans.length - 1];
      if (last && r - (last.start + last.count) < 64) last.count = r - last.start + 1;
      else spans.push({ start: r, count: 1 });
    }
    const where = new Map<number, [TableColumn[], number]>();
    for (const s of spans) {
      const cols = this.gather(idx, s.start, s.count);
      for (let i = 0; i < s.count; i++) where.set(s.start + i, [cols, i]);
    }
    return rows.map((r) => {
      const [cols, i] = where.get(r)!;
      return cols.map((c) => cellOf(c, i));
    });
  }

  /** Rows `start` to `start + count` of the variables at `idx`, read in calls of at most `STEP` rows. */
  private gather(idx: number[], start: number, count: number): TableColumn[] {
    const core = this.open;
    const distinct = [...new Set(idx)].sort((a, b) => a - b);
    const vars = distinct.map((k) => this.variables[k]);
    for (const v of vars) {
      if (v.columnKind === null) throw dtaError("DTA_INVALID", v.notRead ?? `column ${JSON.stringify(v.name)} is not read`, undefined, TypeError);
    }
    // Codecs the module does not read are named before any read.
    const groups = this.info.rowGroups;
    for (const [j, k] of distinct.entries()) {
      for (let g = 0; g < groups.length && count > 0; g++) {
        const { firstRow, rows } = groups[g];
        const codec = vars[j].chunks[g]?.codec;
        if (firstRow < start + count && firstRow + rows > start && codec && !CODECS.has(codec)) {
          const what = codec === "lz4" ? "the Hadoop framing of LZ4 (LZ4_RAW is read)" : `${codec} compression`;
          throw dtaError("DTA_INVALID", `column ${JSON.stringify(this.variables[k].name)}: ${what} is not supported by pqparser`, undefined, TypeError);
        }
      }
    }
    const out: TableColumn[] = vars.map((v) => {
      switch (v.columnKind) {
        case "number":
          return { kind: "number", values: new Float64Array(count), missing: new Uint8Array(count) };
        case "bigint":
          return { kind: "bigint", values: v.type === "uint64" ? new BigUint64Array(count) : new BigInt64Array(count), missing: new Uint8Array(count) };
        case "dictionary":
          return { kind: "dictionary", codes: new Uint32Array(count), values: v.categorical?.levels ? [...v.categorical.levels] : [], missing: new Uint8Array(count) };
        default:
          return { kind: "string", values: new Array<string | Uint8Array>(count), missing: new Uint8Array(count) };
      }
    });
    const levels = vars.map((v) => new Map((v.categorical?.levels ?? []).map((l, i) => [l, i])));
    const fields = Uint32Array.from(distinct);
    const modes = Uint8Array.from(distinct, (k) => this.modes[k]);
    for (const [at, n] of this.calls(start, count)) {
      core.with([new Uint8Array(fields.buffer), modes], ([f, m]) => {
        try {
          core.call((x) => x.pq_read(f, m, distinct.length, start + at, n));
          const dir = core.typed(Uint32Array, core.call((x) => x.pq_directory()), 11 * distinct.length).slice();
          vars.forEach((v, j) => place(core, out[j], dir.subarray(11 * j, 11 * j + 11), at, n, v, levels[j], start));
        } finally {
          core.readEnd();
        }
      });
    }
    // Each position asked for gets its column; a variable asked for again gets a copy.
    const given = new Set<number>();
    return idx.map((k) => {
      const col = out[distinct.indexOf(k)];
      if (!given.has(k)) {
        given.add(k);
        return col;
      }
      return copyColumn(col);
    });
  }

  /**
   * The module calls a read of rows `start` to `start + count` makes, as
   * [first row from `start`, rows]: whole row groups together up to `STEP`
   * rows, a larger one in parts, so that each call holds at most `STEP` rows
   * and a page is rarely decoded twice.
   */
  private calls(start: number, count: number): [number, number][] {
    const out: [number, number][] = [];
    const end = start + count;
    let at = start;
    for (const g of this.info.rowGroups) {
      const groupEnd = Math.min(end, g.firstRow + g.rows);
      if (groupEnd <= at) continue;
      while (at < groupEnd) {
        const last = out[out.length - 1];
        const take = Math.min(groupEnd - at, STEP);
        if (last && last[0] + last[1] === at - start && last[1] + take <= STEP) last[1] += take;
        else out.push([at - start, take]);
        at += take;
      }
      if (at >= end) break;
    }
    return out;
  }

  /** Release the file (and close it, if `openParquetFile` opened it by path). Further reads throw. */
  close(): void {
    if (this.core) {
      this.core = undefined;
      unclosed.unregister(this);
      this.onClose?.();
    }
  }

  [Symbol.dispose](): void {
    this.close();
  }
}

/** Rows per module call, at most: bounds what one call holds. */
const STEP = 1 << 18;
const EMPTY = new Uint8Array(0);
/** Text as distinct values and codes (src/lib.rs's directory), and a code's null. */
const CODED = 4;
const NULL = 0xffffffff;

/** Copy one call's column `d` (src/lib.rs's directory) into `col` from row `at`. */
function place(core: Core, col: TableColumn, d: Uint32Array, at: number, n: number, v: ParquetVariable, levels: Map<string, number>, start: number): void {
  if (col.kind === "number") {
    col.values.set(core.typed(Float64Array, d[1], d[2]), at);
    col.missing.set(core.view(d[3], d[4]), at);
    return;
  }
  if (col.kind === "bigint") {
    if (col.values instanceof BigUint64Array) col.values.set(core.typed(BigUint64Array, d[1], d[2]), at);
    else col.values.set(core.typed(BigInt64Array, d[1], d[2]), at);
    col.missing.set(core.view(d[3], d[4]), at);
    return;
  }
  const bytes = core.view(d[1], d[2]);
  const byteEnds = core.typed(Uint32Array, d[3], d[4]);
  if (d[0] === BYTES) {
    const s = col as StringColumn;
    const missing = core.view(d[5], d[6]);
    s.missing!.set(missing, at);
    let from = 0;
    for (let i = 0; i < n; i++) {
      s.values[at + i] = missing[i] ? EMPTY : bytes.slice(from, byteEnds[i]);
      from = byteEnds[i];
    }
    return;
  }
  // Text: one decode, then a substring per value. Coded (4): the values
  // are the distinct ones the rows use, and each row has a code.
  const text = utf8.decode(bytes);
  const ends = d[8] ? core.typed(Uint32Array, d[7], d[8]) : byteEnds;
  const entries = new Array<string>(ends.length);
  for (let i = 0, from = 0; i < ends.length; i++) {
    entries[i] = text.substring(from, ends[i]);
    from = ends[i];
  }
  const codes = d[0] === CODED ? core.typed(Uint32Array, d[9], d[10]) : undefined;
  const missing = codes ? undefined : core.view(d[5], d[6]);
  const entry = (i: number) => (codes ? (codes[i] === NULL ? -1 : codes[i]) : missing![i] ? -1 : i);
  if (col.kind === "string") {
    for (let i = 0; i < n; i++) {
      const e = entry(i);
      col.values[at + i] = e < 0 ? "" : entries[e];
      col.missing![at + i] = e < 0 ? 1 : 0;
    }
    return;
  }
  // A categorical: codes into the profile's levels, or into the levels
  // found in the rows read, in order of first appearance.
  const fixed = v.categorical?.levels != null;
  const level = new Int32Array(entries.length).fill(-1);
  for (let i = 0; i < n; i++) {
    const e = entry(i);
    if (e < 0) {
      col.missing![at + i] = 1;
      continue;
    }
    if (level[e] < 0) {
      let k = levels.get(entries[e]);
      if (k === undefined) {
        if (fixed) {
          throw dtaError("DTA_INVALID", `column ${JSON.stringify(v.name)}: the value ${JSON.stringify(entries[e])} in row ${start + at + i + 1} is not one of the levels in the dta profile`, undefined, TypeError);
        }
        k = col.values.length;
        col.values.push(entries[e]);
        levels.set(entries[e], k);
      }
      level[e] = k;
    }
    col.codes[at + i] = level[e];
  }
}

/** A column with arrays of its own. */
function copyColumn(c: TableColumn): TableColumn {
  switch (c.kind) {
    case "number":
      return { kind: "number", values: c.values.slice(), missing: c.missing.slice() };
    case "bigint":
      return { kind: "bigint", values: c.values.slice(), missing: c.missing.slice() } as BigIntColumn;
    case "dictionary":
      return { kind: "dictionary", codes: c.codes.slice(), values: [...c.values], missing: c.missing?.slice() };
    default:
      return { kind: "string", values: [...c.values], missing: c.missing?.slice() };
  }
}

/** Row `i` of a column as a cell. */
function cellOf(c: TableColumn, i: number): TableCell {
  switch (c.kind) {
    case "number":
      return c.missing[i] ? missingValue(c.missing[i]) : c.values[i];
    case "bigint":
      return c.missing[i] ? MISSING["."] : c.values[i];
    case "dictionary":
      return c.missing?.[i] ? MISSING["."] : c.values[c.codes[i]];
    default:
      return c.missing?.[i] ? MISSING["."] : c.values[i];
  }
}

/** Open a Parquet file held in memory. Only the footer is read now. */
export function openParquet(bytes: Uint8Array, options: ParquetOptions = {}): ParquetReader {
  return new ParquetReader(
    (into, offset) => {
      if (offset < 0 || offset + into.length > bytes.length) throw new RangeError("read past the end of the file");
      into.set(bytes.subarray(offset, offset + into.length));
    },
    bytes.length,
    options,
  );
}

export type { MissingName };
