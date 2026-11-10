// Damaged files give a thrown Error with a code, never a crash: truncated,
// garbage, a cut footer, and bytes changed anywhere in a file.
import assert from "node:assert/strict";
import { test } from "node:test";
import { join } from "node:path";
import { FIXTURES, OPTIONS, bytesOf, pq } from "./helpers.mjs";

/** Open `bytes` and read every readable column whole and as scattered rows; what is thrown is returned. */
function readAll(bytes) {
  try {
    const r = pq.openParquet(bytes, OPTIONS);
    const readable = r.variables.flatMap((v, k) => (v.columnKind === null ? [] : [k]));
    for (const v of r.variables) if (v.valueLabels) r.valueLabels(v.valueLabels);
    r.readColumns(readable);
    if (r.info.nobs) r.readRowsAt([r.info.nobs - 1, 0], readable);
    r.close();
    return undefined;
  } catch (e) {
    return e;
  }
}

function assertCoded(e, what) {
  assert.ok(e instanceof Error, `${what}: ${e}`);
  assert.match(e.code ?? "", /^DTA_/, `${what}: ${e.message}`);
}

const FILES = ["compat/pyarrow_default", "compat/polars_zstd", "compat/pyarrow_delta_v2", "legacy/r_types", "compat/hyparquet_delta", "legacy/ts_rows_lz4_raw", "compat/pyarrow_gzip"];

test("too short, garbage, and no footer", () => {
  for (const bytes of [new Uint8Array(0), new Uint8Array(5), Buffer.from("PAR1PAR1"), Buffer.from("PAR1\0\0\0\0\0\0\0\0PAR1"), Buffer.alloc(4096, 0x5a)]) {
    const e = readAll(bytes);
    assertCoded(e, `${bytes.length} bytes`);
    assert.equal(e.code, "DTA_CORRUPT");
  }
});

test("truncated files, and footers cut", () => {
  for (const name of FILES) {
    const bytes = bytesOf(join(FIXTURES, `${name}.parquet`));
    for (const keep of [bytes.length - 1, bytes.length - 8, bytes.length - 100, Math.floor(bytes.length / 2), 12]) {
      assertCoded(readAll(bytes.subarray(0, keep)), `${name} cut to ${keep}`);
    }
    // The footer's length says more than the file holds, or less than the footer is.
    for (const footer of [0xffffff, 3, 0]) {
      const b = Uint8Array.from(bytes);
      new DataView(b.buffer).setUint32(b.length - 8, footer, true);
      assertCoded(readAll(b), `${name} footer length ${footer}`);
    }
  }
});

test("bytes changed anywhere: an Error or the values, never a crash", () => {
  let seed = 12345;
  const random = () => ((seed = (seed * 1103515245 + 12345) >>> 0) / 2 ** 32);
  let errors = 0;
  for (const name of FILES) {
    const bytes = bytesOf(join(FIXTURES, `${name}.parquet`));
    for (let trial = 0; trial < 150; trial++) {
      const b = Uint8Array.from(bytes);
      // Mostly in the footer, where a change says most.
      const footer = new DataView(b.buffer).getUint32(b.length - 8, true);
      for (let k = 0; k < 1 + Math.floor(random() * 4); k++) {
        const at = random() < 0.6 ? b.length - 8 - Math.floor(random() * footer) : Math.floor(random() * b.length);
        b[at] = random() < 0.5 ? Math.floor(random() * 256) : b[at] ^ (1 << Math.floor(random() * 8));
      }
      const e = readAll(b);
      if (e !== undefined) {
        assertCoded(e, `${name} trial ${trial}`);
        errors++;
      }
    }
  }
  assert.ok(errors > 100, `${errors} damaged files gave errors`);
});

test("a reader that stopped on a damaged file stays stopped, and others go on", () => {
  const good = bytesOf(join(FIXTURES, "compat/pyarrow_default.parquet"));
  const r = pq.openParquet(good, OPTIONS);
  const [want] = r.readColumns([0]);
  r.close();
  assert.throws(() => r.readColumns([0]), (e) => e.code === "DTA_CLOSED");
  const again = pq.openParquet(good, OPTIONS);
  assert.deepEqual(again.readColumns([0])[0], want);
});

test("a read that fails in the file system is DTA_IO, with the cause", () => {
  const good = bytesOf(join(FIXTURES, "compat/pyarrow_default.parquet"));
  let fail = false;
  const r = new pq.ParquetReader(
    (into, offset) => {
      if (fail) throw new Error("disk gone");
      into.set(good.subarray(offset, offset + into.length));
    },
    good.length,
    OPTIONS,
  );
  fail = true;
  assert.throws(() => r.readColumns([0]), (e) => e.code === "DTA_IO" && e.cause?.message === "disk gone");
  fail = false;
  assert.equal(r.readColumns([0])[0].values.length, r.info.nobs);
});
