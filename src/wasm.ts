// The wasm module (src/lib.rs): compiled once, instantiated once per
// reader, so a module that stops (a trap) takes only its own reader with it.

import { brotliDecompressSync } from "node:zlib";
import { dtaError } from "./errors";
import type { ReadAt } from "./parquet";

interface Exports {
  memory: WebAssembly.Memory;
  pq_alloc(n: number): number;
  pq_free(p: number, n: number): void;
  pq_result(): number;
  pq_result_len(): number;
  pq_open(len: number): number;
  pq_meta(): number;
  pq_key_value(k: number): number;
  pq_read(fields: number, modes: number, n: number, start: number, count: number): number;
  pq_directory(): number;
  pq_read_end(): void;
}

let compiled: WebAssembly.Module | undefined;

function wasmModule(): WebAssembly.Module {
  if (!compiled) {
    // Written by scripts/build.mjs: the module, brotli-compressed, in base64.
    const data: string = require("./wasm-data.js");
    compiled = new WebAssembly.Module(brotliDecompressSync(Buffer.from(data, "base64")));
  }
  return compiled;
}

const utf8 = new TextDecoder("utf-8", { ignoreBOM: true });

/** One instance of the module, reading one file. */
export class Core {
  private readonly x: Exports;
  /** What `read` threw during the current call. */
  private failed?: unknown;
  /** Why the instance stopped, once it has. */
  private stopped?: string;

  constructor(read: ReadAt) {
    const imports = {
      env: {
        host_read: (ptr: number, len: number, offset: number): number => {
          try {
            read(new Uint8Array(this.x.memory.buffer, ptr, len), offset);
            return 0;
          } catch (e) {
            this.failed = e;
            return 1;
          }
        },
      },
    };
    this.x = new WebAssembly.Instance(wasmModule(), imports).exports as unknown as Exports;
  }

  /** Call `f`; -1 is the module's error (its message is the result), a trap stops the instance. */
  call(f: (x: Exports) => number): number {
    if (this.stopped) throw dtaError("DTA_CORRUPT", `the reader stopped earlier: ${this.stopped}`);
    this.failed = undefined;
    let r: number;
    try {
      r = f(this.x);
    } catch (e) {
      if (!(e instanceof WebAssembly.RuntimeError)) throw e;
      // A panic's message, if the hook left one (see src/lib.rs).
      let why = e.message;
      try {
        const text = this.text();
        if (text.startsWith("panicked at")) why = text.replace(/\s+/g, " ");
      } catch {
        // The memory is as the trap left it.
      }
      this.stopped = why;
      throw dtaError("DTA_CORRUPT", `the Parquet reader stopped on this file (${why})`, e);
    }
    if (this.failed !== undefined) throw dtaError("DTA_IO", "reading the file failed", this.failed);
    // -1: the file is damaged; -2: a column cannot be read as asked (src/lib.rs).
    if (r === -2) throw dtaError("DTA_INVALID", this.text(), undefined, TypeError);
    if (r < 0) throw dtaError("DTA_CORRUPT", this.text());
    return r;
  }

  /** The module's last result as text. */
  text(): string {
    return utf8.decode(this.bytes());
  }

  /** The module's last result as bytes, copied out. */
  bytes(): Uint8Array {
    return this.view(this.x.pq_result(), this.x.pq_result_len()).slice();
  }

  view(ptr: number, len: number): Uint8Array {
    return new Uint8Array(this.x.memory.buffer, ptr, len);
  }

  /** `n` values of type `T` at `ptr`, viewed in place. */
  typed<T>(Kind: { new (b: ArrayBuffer, at: number, n: number): T }, ptr: number, n: number): T {
    return new Kind(this.x.memory.buffer as ArrayBuffer, ptr, n);
  }

  /** Copy `bytes` into the module, call `f` with where they are, and free them. */
  with<T>(bytes: Uint8Array[], f: (ptrs: number[]) => T): T {
    const ptrs = bytes.map((b) => {
      const p = this.call((x) => x.pq_alloc(Math.max(1, b.length)));
      this.view(p, b.length).set(b);
      return p;
    });
    try {
      return f(ptrs);
    } finally {
      if (!this.stopped) ptrs.forEach((p, k) => this.x.pq_free(p, Math.max(1, bytes[k].length)));
    }
  }

  /** Free what the last read kept, unless the instance stopped. */
  readEnd(): void {
    if (!this.stopped) this.x.pq_read_end();
  }

  get exports(): Exports {
    return this.x;
  }
}
