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
