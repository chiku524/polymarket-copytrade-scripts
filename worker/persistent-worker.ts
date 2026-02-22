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

const DEFAULT_INTERVAL_MS = 15000;
const DEFAULT_TIMEOUT_MS = 70000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeBaseUrl(url: string): string {
  return url.trim().replace(/\/+$/, "");
}

function resolveTargetUrl(): string {
  const direct = process.env.WORKER_TARGET_URL?.trim();
  if (direct) return direct;

  const appBase =
    process.env.APP_BASE_URL?.trim() ||
    (process.env.RAILWAY_PUBLIC_DOMAIN
      ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`
      : undefined) ||
    (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : undefined);

  if (!appBase) {
    throw new Error(
      "Set APP_BASE_URL (or WORKER_TARGET_URL) so the worker knows where to call /api/copy-trade."
    );
  }
  return `${normalizeBaseUrl(appBase)}/api/copy-trade`;
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
  const targetUrl = resolveTargetUrl();
  const cronSecret = process.env.CRON_SECRET?.trim();
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
  console.log(`[worker] target=${targetUrl}`);
  console.log(`[worker] intervalMs=${intervalMs}, timeoutMs=${timeoutMs}`);

  while (running) {
    const started = Date.now();
    try {
      const headers: Record<string, string> = { Accept: "application/json" };
      if (cronSecret) headers.authorization = `Bearer ${cronSecret}`;

      const res = await fetchWithTimeout(
        targetUrl,
        {
          method: "GET",
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
