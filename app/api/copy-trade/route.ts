import { NextRequest, NextResponse } from "next/server";
import { getConfig, getState, setState, appendActivity } from "@/lib/kv";
import { runCopyTrade } from "@/lib/copy-trade";

const PRIVATE_KEY = process.env.PRIVATE_KEY;
const MY_ADDRESS = process.env.MY_ADDRESS ?? "0x370e81c93aa113274321339e69049187cce03bb9";
const TARGET_ADDRESS = process.env.TARGET_ADDRESS ?? "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d";
const SIGNATURE_TYPE = parseInt(process.env.SIGNATURE_TYPE ?? "1", 10);
const CRON_SECRET = process.env.CRON_SECRET;

export const maxDuration = 60;

async function runCopyTradeHandler() {

  if (!PRIVATE_KEY) {
    return NextResponse.json({ error: "PRIVATE_KEY not configured" }, { status: 500 });
  }

  try {
    const config = await getConfig();
    if (!config.enabled) {
      return NextResponse.json({ ok: true, skipped: true, reason: "disabled" });
    }

    const state = await getState();
    const result = await runCopyTrade(
      PRIVATE_KEY,
      MY_ADDRESS,
      TARGET_ADDRESS,
      SIGNATURE_TYPE,
      {
        copyPercent: config.copyPercent,
        maxBetUsd: config.maxBetUsd,
        minBetUsd: config.minBetUsd,
      },
      { lastTimestamp: state.lastTimestamp, copiedKeys: state.copiedKeys }
    );

    await setState({
      lastTimestamp: result.lastTimestamp ?? state.lastTimestamp,
      copiedKeys: result.copiedKeys.length > 0 ? result.copiedKeys : state.copiedKeys,
      lastRunAt: Date.now(),
      lastCopiedAt: result.copied > 0 ? Date.now() : state.lastCopiedAt,
      lastError: result.error,
    });
    if (result.copiedTrades?.length) {
      await appendActivity(result.copiedTrades);
    }

    return NextResponse.json({
      ok: true,
      copied: result.copied,
      failed: result.failed,
      error: result.error,
    });
  } catch (e) {
    const err = e instanceof Error ? e.message : String(e);
    console.error("Copy trade error:", e);
    return NextResponse.json({ ok: false, error: err }, { status: 500 });
  }
}

export async function GET(request: NextRequest) {
  if (CRON_SECRET) {
    const auth = request.headers.get("authorization");
    if (auth !== `Bearer ${CRON_SECRET}`) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }
  }
  return runCopyTradeHandler();
}

export async function POST() {
  // Cron uses GET; POST reserved for future use
  return NextResponse.json({ error: "Use GET for cron" }, { status: 405 });
}
