// Compile the TypeScript to dist/, and embed build/pqparser.wasm (made by
// scripts/build-wasm.sh) in dist/wasm-data.js, brotli-compressed, as base64.
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { brotliCompressSync, constants } from "node:zlib";

const require = createRequire(import.meta.url);
execFileSync(process.execPath, [require.resolve("typescript/bin/tsc"), "-p", "tsconfig.json"], { stdio: "inherit" });
const wasm = readFileSync(new URL("../build/pqparser.wasm", import.meta.url));
const packed = brotliCompressSync(wasm, { params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_SIZE_HINT]: wasm.length } });
writeFileSync(new URL("../dist/wasm-data.js", import.meta.url), `module.exports = ${JSON.stringify(packed.toString("base64"))};\n`);
console.log(`dist/wasm-data.js: the module, ${wasm.length} bytes, ${packed.length} brotli-compressed`);
