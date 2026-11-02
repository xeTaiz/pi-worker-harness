// Bun test preload: tests must never read the developer's real Worker Harness
// config or API key (~/.config/worker-harness/api-key, ~/.omp/worker-harness).
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

process.env.HOME = mkdtempSync(join(tmpdir(), "wh-ext-test-home-"));
delete process.env.USERPROFILE;
