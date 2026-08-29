import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

export interface WorkerHarnessConfig {
  orchestratorUrl?: string;
}

function homeDir(): string {
  const home = process.env.HOME ?? process.env.USERPROFILE;
  if (!home) throw new Error("Could not resolve user home directory for worker-harness config.");
  return home;
}

export function harnessAgent(): "pi" | "omp" {
  return basename(process.execPath).toLowerCase().startsWith("omp") ? "omp" : "pi";
}

export function getWorkerHarnessConfigPath(): string {
  return join(homeDir(), harnessAgent() === "omp" ? ".omp" : ".pi", "worker-harness", "config.json");
}

async function readConfig(path: string): Promise<WorkerHarnessConfig | null> {
  try {
    const raw = await readFile(path, "utf8");
    const parsed = JSON.parse(raw) as WorkerHarnessConfig;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (err) {
    if (err && typeof err === "object" && "code" in err && err.code === "ENOENT") {
      return null;
    }
    throw err;
  }
}

export async function loadWorkerHarnessConfig(): Promise<WorkerHarnessConfig> {
  const primary = await readConfig(getWorkerHarnessConfigPath());
  if (primary) return primary;
  return (await readConfig(join(homeDir(), ".pi", "worker-harness", "config.json"))) ?? {};
}

export async function saveWorkerHarnessConfig(config: WorkerHarnessConfig): Promise<void> {
  const path = getWorkerHarnessConfigPath();
  const dir = dirname(path);
  await mkdir(dir, { recursive: true });

  const tempPath = `${path}.tmp-${process.pid}-${Date.now()}`;
  const serialized = JSON.stringify(config, null, 2) + "\n";

  await writeFile(tempPath, serialized, { encoding: "utf8", mode: 0o600 });
  await rename(tempPath, path);
}
