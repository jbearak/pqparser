// Cells and missing values, as @jbearak/dta-parser has them (copied from
// its js/src/values.ts, same author and license).

/** The name of a Stata missing value: `.` or `.a` to `.z`. */
export type MissingName =
  | "."
  | ".a" | ".b" | ".c" | ".d" | ".e" | ".f" | ".g" | ".h" | ".i" | ".j" | ".k" | ".l" | ".m"
  | ".n" | ".o" | ".p" | ".q" | ".r" | ".s" | ".t" | ".u" | ".v" | ".w" | ".x" | ".y" | ".z";

/** A Stata missing value: `.` or `.a` to `.z`. */
export interface Missing {
  readonly missing: MissingName;
}

export const MISSING_NAMES = [".", ...Array.from("abcdefghijklmnopqrstuvwxyz", (c) => `.${c}`)] as MissingName[];

/**
 * The 27 missing values, one frozen object each, so `===` compares them:
 * `MISSING["."]`, `MISSING[".a"]`, ... `MISSING[".z"]`.
 *
 * Made once per JavaScript realm: every copy of this code (pqparser,
 * @jbearak/dta-parser 2, a second installed copy) finds the objects under
 * one `Symbol.for` key on `globalThis`, so a `ValueLabels` map one of them
 * read is keyed by the objects every other one hands out.
 * What is found there is used only if it has exactly this layout; `v1`
 * names the layout, and a different one would take a new key.
 */
const KEY = Symbol.for("@jbearak/dta-parser/missing/v1");

export const MISSING: Readonly<Record<string, Missing>> = shared() ?? register();

function make(): Readonly<Record<string, Missing>> {
  return Object.freeze(Object.fromEntries(MISSING_NAMES.map((name) => [name, Object.freeze({ missing: name })])));
}


/** The realm's objects, if another copy made them and they are what this one would make. */
function shared(): Readonly<Record<string, Missing>> | undefined {
  const found = (globalThis as Record<symbol, unknown>)[KEY];
  if (typeof found !== "object" || found === null || !Object.isFrozen(found)) return undefined;
  const keys = Object.keys(found);
  if (keys.length !== MISSING_NAMES.length || keys.some((k, i) => k !== MISSING_NAMES[i])) return undefined;
  for (const name of MISSING_NAMES) {
    const m = (found as Record<string, unknown>)[name];
    if (typeof m !== "object" || m === null || !Object.isFrozen(m)) return undefined;
    if (Object.keys(m).length !== 1 || (m as Missing).missing !== name) return undefined;
  }
  return found as Readonly<Record<string, Missing>>;
}

/** Make the objects and put them where other copies look, unless something else holds the key. */
function register(): Readonly<Record<string, Missing>> {
  const made = make();
  const g = globalThis as Record<symbol, unknown>;
  if (!(KEY in g)) {
    try {
      Object.defineProperty(g, KEY, { value: made, enumerable: false, writable: false, configurable: false });
    } catch {
      // A frozen globalThis: this copy keeps its own objects.
    }
  }
  return made;
}

/** The missing value for a missing code (1 `.`, 2 to 27 `.a` to `.z`), as a `NumberColumn`'s `missing` holds it. */
export function missingValue(code: number): Missing {
  return MISSING[MISSING_NAMES[code - 1]];
}

/** Whether a cell is a missing value (`.` or `.a` to `.z`) rather than a number, string or bytes. */
export function isMissing(value: unknown): value is Missing {
  return typeof value === "object" && value !== null && "missing" in value;
}

/** One cell: a number, a string, a binary strL, or a missing value. */
export type Cell = number | string | Uint8Array | Missing;

/** Value label set: maps a value, or a missing value, to its text. */
export type ValueLabels = Map<number | Missing, string>;
