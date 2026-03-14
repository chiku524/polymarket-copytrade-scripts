const DEFAULT_INTERVAL_MS = 15000;
const DEFAULT_DURATION_MINUTES = 120;
const DEFAULT_SWITCH_EVERY_RUNS = 1;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeBaseUrl(url) {
  return String(url || "").trim().replace(/\/+$/, "");
}

function parseJsonEnv(name, fallback) {
  const raw = process.env[name];
  if (!raw) return fallback;
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return fallback;
    return parsed;
  } catch {
    console.warn(`[paper-ab] ${name} is not valid JSON; using fallback.`);
    return fallback;
  }
}

async function fetchJson(url, init = {}) {
  const res = await fetch(url, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      ...(init.headers || {}),
    },
    cache: "no-store",
  });
  const text = await res.text();
  let json = {};
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function profileSummary(profile) {
  const runs = profile.runs;
  const avg = (x) => (runs > 0 ? x / runs : 0);
  return {
    runs,
    errors: profile.errors,
    totalPairs: profile.totalPairs,
    totalEligible: profile.totalEligible,
    totalEvaluated: profile.totalEvaluated,
    totalBudgetUsedUsd: Number(profile.totalBudgetUsedUsd.toFixed(2)),
    avgPairsPerRun: Number(avg(profile.totalPairs).toFixed(3)),
    avgEligiblePerRun: Number(avg(profile.totalEligible).toFixed(3)),
    avgBudgetUsedUsd: Number(avg(profile.totalBudgetUsedUsd).toFixed(3)),
    avgExecutedNetEdgeCents:
      profile.totalPairs > 0
        ? Number((profile.netEdgePairWeighted / profile.totalPairs).toFixed(4))
        : 0,
  };
}

async function main() {
  const appBase = normalizeBaseUrl(process.env.APP_BASE_URL);
  if (!appBase) {
    throw new Error("APP_BASE_URL is required (example: https://your-app.fly.dev)");
  }
  const intervalMs = Math.max(1000, toNum(process.env.AB_INTERVAL_MS, DEFAULT_INTERVAL_MS));
  const durationMinutes = Math.max(
    5,
    toNum(process.env.AB_DURATION_MINUTES, DEFAULT_DURATION_MINUTES)
  );
  const switchEveryRuns = Math.max(
    1,
    Math.floor(toNum(process.env.AB_SWITCH_EVERY_RUNS, DEFAULT_SWITCH_EVERY_RUNS))
  );
  const maxRuns = Math.max(1, Math.floor((durationMinutes * 60 * 1000) / intervalMs));

  const defaultProfileA = {
    pairMinEdgeCents5m: 0.6,
    pairMinEdgeCents15m: 0.7,
    reentryMaxEntriesPerSignal: 2,
    reentryEdgeStepCents: 0.15,
    dynamicSizingMaxScalePct: 130,
  };
  const defaultProfileB = {
    pairMinEdgeCents5m: 0.4,
    pairMinEdgeCents15m: 0.5,
    reentryMaxEntriesPerSignal: 3,
    reentryEdgeStepCents: 0.1,
    dynamicSizingMaxScalePct: 160,
  };
  const profileA = parseJsonEnv("AB_PROFILE_A_JSON", defaultProfileA);
  const profileB = parseJsonEnv("AB_PROFILE_B_JSON", defaultProfileB);

  const commonPatch = {
    mode: "paper",
    walletUsagePercent: 100,
    maxRunBudgetUsd: 0,
    pairMaxMarketsPerRun: 20,
    enableCadenceHourly: false,
    paperAllowNegativeEdge: false,
  };

  const metrics = {
    A: {
      runs: 0,
      errors: 0,
      totalPairs: 0,
      totalEligible: 0,
      totalEvaluated: 0,
      totalBudgetUsedUsd: 0,
      netEdgePairWeighted: 0,
    },
    B: {
      runs: 0,
      errors: 0,
      totalPairs: 0,
      totalEligible: 0,
      totalEvaluated: 0,
      totalBudgetUsedUsd: 0,
      netEdgePairWeighted: 0,
    },
  };

  console.log(
    `[paper-ab] start base=${appBase} runs=${maxRuns} intervalMs=${intervalMs} switchEveryRuns=${switchEveryRuns}`
  );

  for (let runIdx = 0; runIdx < maxRuns; runIdx++) {
    const startedAt = Date.now();
    const profileKey = Math.floor(runIdx / switchEveryRuns) % 2 === 0 ? "A" : "B";
    const profilePatch = profileKey === "A" ? profileA : profileB;
    const appliedPatch = { ...commonPatch, ...profilePatch };

    try {
      const cfgRes = await fetchJson(`${appBase}/api/config`, {
        method: "PATCH",
        body: JSON.stringify(appliedPatch),
      });
      if (!cfgRes.res.ok) {
        metrics[profileKey].errors += 1;
        console.error(
          `[paper-ab] run=${runIdx + 1} profile=${profileKey} config PATCH failed`,
          cfgRes.json
        );
      } else {
        const runRes = await fetchJson(`${appBase}/api/run-now`, { method: "POST" });
        const payload = runRes.json || {};
        metrics[profileKey].runs += 1;
        metrics[profileKey].totalPairs += toNum(payload.paper, toNum(payload.copied, 0));
        metrics[profileKey].totalEligible += toNum(payload.eligibleSignals, 0);
        metrics[profileKey].totalEvaluated += toNum(payload.evaluatedSignals, 0);
        metrics[profileKey].totalBudgetUsedUsd += toNum(payload.budgetUsedUsd, 0);
        const executedPairs = toNum(payload.paper, toNum(payload.copied, 0));
        const avgNet = toNum(payload.avgExecutedNetEdgeCents, 0);
        metrics[profileKey].netEdgePairWeighted += executedPairs * avgNet;

        if (!runRes.res.ok) {
          metrics[profileKey].errors += 1;
          console.error(
            `[paper-ab] run=${runIdx + 1} profile=${profileKey} run-now failed`,
            payload
          );
        } else {
          console.log(
            `[paper-ab] run=${runIdx + 1}/${maxRuns} profile=${profileKey} pairs=${executedPairs} eligible=${toNum(payload.eligibleSignals, 0)} eval=${toNum(payload.evaluatedSignals, 0)} netEdge=${avgNet.toFixed(3)} budget=$${toNum(payload.budgetUsedUsd, 0).toFixed(2)}`
          );
        }
      }
    } catch (error) {
      metrics[profileKey].errors += 1;
      console.error(
        `[paper-ab] run=${runIdx + 1} profile=${profileKey} exception:`,
        error instanceof Error ? error.message : String(error)
      );
    }

    const elapsed = Date.now() - startedAt;
    const waitMs = Math.max(250, intervalMs - elapsed);
    if (runIdx < maxRuns - 1) {
      await sleep(waitMs);
    }
  }

  const summary = {
    A: profileSummary(metrics.A),
    B: profileSummary(metrics.B),
  };
  console.log("[paper-ab] complete summary:");
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  console.error("[paper-ab] fatal:", error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
