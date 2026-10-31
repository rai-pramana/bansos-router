// Loaded via --import before any test module so BANSOS_DIR is already set when
// state.ts/activity.ts evaluate their module-level paths.
//
// No imports from src/ here, deliberately: ESM evaluates a module's imports
// before its body, so importing anything that reaches state.ts would resolve
// BANSOS_DIR against the real home before the assignment below ever ran.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

if (!process.env.BANSOS_DIR) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bansos-test-"));
    process.env.BANSOS_DIR = dir;
    process.on("exit", () => fs.rmSync(dir, { recursive: true, force: true }));
}

// A test must never write to the live daemon's state directory.
//
// The isolation above is a CONVENTION: it only holds when the suite runs
// through `npm test`, which passes --import ./test/isolate.ts. Run a single
// file directly - `npx tsx --test test/relay-retry.test.ts` - and the
// preload never happens, so BANSOS_DIR stays at ~/.bansos and the test's
// cleanup deletes the live daemon's files.
//
// That happened on 2026-10-31: a test run without the preload removed
// ~/.bansos/quota-evidence.json, and the running daemon restarted into an
// empty spent-evidence map - losing the "this fleet is spent" shortcut that
// keeps a closed lane failing in 1ms instead of 30s.
//
// So the guard is a hard refusal, not a convention: a test that would run
// against the real state directory exits instead of touching it.
const real = path.join(os.homedir(), ".bansos");
if (path.resolve(process.env.BANSOS_DIR!) === path.resolve(real)) {
    console.error(
        "refusing to run: BANSOS_DIR points at the live daemon state directory.\n" +
            `  BANSOS_DIR = ${process.env.BANSOS_DIR}\n` +
            "  run the suite through `npm test` (it loads test/isolate.ts), or set\n" +
            "  BANSOS_DIR to a temporary directory yourself.",
    );
    process.exit(1);
}
