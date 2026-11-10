// Time a whole-file readColumns with pqparser and with @jbearak/dta-parser
// 1.1.0: the first call in a fresh process (loading the module included),
// then the median of warm runs. node scripts/bench.mjs FILE.parquet [RUNS]
import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const [file, runs = "15", which] = process.argv.slice(2);
const libs = { pqparser: require.resolve("../dist/node.js"), "dta-parser 1.1.0": require.resolve("@jbearak/dta-parser/node") };

if (which) {
  // One process: the first call, then warm runs.
  const t0 = performance.now();
  const { openParquetFile } = require(libs[which]);
  const read = () => {
    const r = openParquetFile(file, { nested: "error", int64: "auto", pageCacheBytes: 32 << 20 });
    const cols = r.variables.flatMap((v, k) => (v.columnKind === null ? [] : [k]));
    r.readColumns(cols);
    r.close();
  };
  read();
  const first = performance.now() - t0;
  const times = [];
  for (let i = 0; i < +runs; i++) {
    const t = performance.now();
    read();
    times.push(performance.now() - t);
  }
  times.sort((a, b) => a - b);
  console.log(JSON.stringify({ first, warm: times[times.length >> 1] }));
} else {
  for (const name of Object.keys(libs)) {
    const out = JSON.parse(execFileSync(process.execPath, [new URL(import.meta.url).pathname, file, runs, name], { encoding: "utf8" }));
    console.log(`${name}: first call ${out.first.toFixed(1)} ms (module load included), warm median ${out.warm.toFixed(2)} ms`);
  }
}
