type WorkerResult =
  | {
      ok?: boolean;
      skipped?: boolean;
      reason?: string;
      mode?: "off" | "paper" | "live";
      copied?: number;
      paper?: number;
      failed?: number;
      budgetCapUsd?: number;
      budgetUsedUsd?: number;
      error?: string;
    }
  | Record<string, unknown>;

const DEFAULT_INTERVAL_MS = 10000;
const DEFAULT_TIMEOUT_MS = 70000;

type WorkerTargetMethod = "GET" | "POST";
type WorkerTarget = {
  url: string;
  method: WorkerTargetMethod;
  requireCronAuth: boolean;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeBaseUrl(url: string): string {
  return url.trim().replace(/\/+$/, "");
}

function parseMethod(raw: string | undefined, fallback: WorkerTargetMethod): WorkerTargetMethod {
  const method = String(raw ?? "").trim().toUpperCase();
  return method === "GET" || method === "POST" ? method : fallback;
}

function resolveTarget(): WorkerTarget {
  const direct = process.env.WORKER_TARGET_URL?.trim();
  if (direct) {
    const methodDefault = direct.includes("/api/copy-trade") ? "GET" : "POST";
    const method = parseMethod(process.env.WORKER_TARGET_METHOD, methodDefault);
    const requireCronAuth = direct.includes("/api/copy-trade");
    return {
      url: direct,
      method,
      requireCronAuth,
    };
  }

  const appBase = process.env.APP_BASE_URL?.trim();
  if (!appBase) {
    throw new Error(
      "Set APP_BASE_URL (or WORKER_TARGET_URL) so the worker knows where to call the strategy endpoint."
    );
  }
  // Default to /api/run-now. When WORKER_SHARED_SECRET is set on the web
  // app, non-browser callers must provide x-worker-shared-secret.
  return {
    url: `${normalizeBaseUrl(appBase)}/api/run-now`,
    method: "POST",
    requireCronAuth: false,
  };
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  timeoutMs: number
): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal, cache: "no-store" });
  } finally {
    clearTimeout(timeout);
  }
}

function asMoney(v: unknown): string {
  const n = Number(v);
  if (!Number.isFinite(n)) return "n/a";
  return `$${n.toFixed(2)}`;
}

function toJson(text: string): WorkerResult {
  try {
    return text ? (JSON.parse(text) as WorkerResult) : {};
  } catch {
    return { raw: text };
  }
}

async function main(): Promise<void> {
  const target = resolveTarget();
  const cronSecret = process.env.CRON_SECRET?.trim();
  const workerSharedSecret = process.env.WORKER_SHARED_SECRET?.trim() || cronSecret;
  const intervalMs = Math.max(
    1000,
    Number.parseInt(process.env.WORKER_INTERVAL_MS ?? String(DEFAULT_INTERVAL_MS), 10) ||
      DEFAULT_INTERVAL_MS
  );
  const timeoutMs = Math.max(
    5000,
    Number.parseInt(
      process.env.WORKER_REQUEST_TIMEOUT_MS ?? String(DEFAULT_TIMEOUT_MS),
      10
    ) || DEFAULT_TIMEOUT_MS
  );

  let running = true;
  const stop = (signal: string) => {
    console.log(`[worker] Received ${signal}. Stopping after current cycle...`);
    running = false;
  };
  process.on("SIGINT", () => stop("SIGINT"));
  process.on("SIGTERM", () => stop("SIGTERM"));

  console.log(`[worker] Starting persistent strategy worker`);
  console.log(`[worker] target=${target.url}`);
  console.log(`[worker] method=${target.method}, requireCronAuth=${target.requireCronAuth}`);
  console.log(`[worker] hasSharedSecret=${workerSharedSecret ? "yes" : "no"}`);
  console.log(`[worker] intervalMs=${intervalMs}, timeoutMs=${timeoutMs}`);

  while (running) {
    const started = Date.now();
    try {
      const headers: Record<string, string> = { Accept: "application/json" };
      if (target.method === "POST") headers["Content-Type"] = "application/json";
      if (workerSharedSecret) {
        headers["x-worker-shared-secret"] = workerSharedSecret;
      }
      if (target.requireCronAuth) {
        if (cronSecret) {
          headers.authorization = `Bearer ${cronSecret}`;
        } else {
          console.warn("[worker] target requires CRON_SECRET but CRON_SECRET is missing");
        }
      }

      const res = await fetchWithTimeout(
        target.url,
        {
          method: target.method,
          headers,
        },
        timeoutMs
      );
      const bodyText = await res.text();
      const payload = toJson(bodyText);
      const elapsedMs = Date.now() - started;

      if (!res.ok) {
        const errMsg =
          (payload as { error?: string }).error ??
          `HTTP ${res.status} ${res.statusText}`.trim();
        console.error(`[worker] ERROR ${errMsg} (${elapsedMs}ms)`);
      } else if ((payload as { skipped?: boolean }).skipped) {
        const reason = (payload as { reason?: string }).reason ?? "skipped";
        console.log(`[worker] skipped=${reason} (${elapsedMs}ms)`);
      } else {
        const p = payload as {
          mode?: string;
          copied?: number;
          paper?: number;
          failed?: number;
          budgetUsedUsd?: number;
          budgetCapUsd?: number;
          error?: string;
        };
        console.log(
          `[worker] mode=${p.mode ?? "unknown"} copied=${p.copied ?? 0} paper=${p.paper ?? 0} failed=${p.failed ?? 0} budget=${asMoney(p.budgetUsedUsd)}/${asMoney(p.budgetCapUsd)}${p.error ? ` error=${p.error}` : ""} (${elapsedMs}ms)`
        );
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error(`[worker] request failed: ${msg}`);
    }

    const elapsed = Date.now() - started;
    const waitMs = Math.max(250, intervalMs - elapsed);
    if (!running) break;
    await sleep(waitMs);
  }

  console.log("[worker] Stopped.");
}

main().catch((e) => {
  const msg = e instanceof Error ? e.message : String(e);
  console.error(`[worker] fatal: ${msg}`);
  process.exitCode = 1;
});
