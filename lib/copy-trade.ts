import { ClobClient, OrderType, Side } from "@polymarket/clob-client";
import { Wallet } from "ethers";
import JSZip from "jszip";

const DATA_API = "https://data-api.polymarket.com";
const CLOB_HOST = "https://clob.polymarket.com";
const CHAIN_ID = 137;

export async function getCashBalance(address: string): Promise<number> {
  const res = await fetch(
    `${DATA_API}/v1/accounting/snapshot?user=${encodeURIComponent(address)}`
  );
  if (!res.ok) throw new Error(`Snapshot failed: ${res.status}`);
  const buf = await res.arrayBuffer();
  const zip = await JSZip.loadAsync(buf);
  const equityFile = zip.file("equity.csv");
  if (!equityFile) return 0;
  const text = await equityFile.async("string");
  const lines = text.trim().split("\n");
  if (lines.length < 2) return 0;
  const headers = lines[0].split(",");
  const values = lines[1].split(",");
  const cashIdx = headers.indexOf("cashBalance");
  if (cashIdx < 0) return 0;
  return parseFloat(values[cashIdx] ?? "0") || 0;
}

export interface TradeActivity {
  type: string;
  timestamp: number;
  transactionHash: string;
  asset: string;
  side: string;
  price: number;
  size: number;
  usdcSize?: number;
  title: string;
  outcome?: string;
}

export async function getTargetActivity(
  address: string,
  limit = 50
): Promise<TradeActivity[]> {
  const params = new URLSearchParams({
    user: address,
    type: "TRADE",
    limit: String(limit),
    sortBy: "TIMESTAMP",
    sortDirection: "DESC",
  });
  const res = await fetch(`${DATA_API}/activity?${params}`);
  if (!res.ok) throw new Error(`Activity failed: ${res.status}`);
  const data = (await res.json()) as TradeActivity[];
  return (Array.isArray(data) ? data : []).map((a) => ({
    ...a,
    price: parseFloat(String(a.price ?? 0)) || 0,
    size: parseFloat(String(a.size ?? 0)) || 0,
    usdcSize: a.usdcSize != null ? parseFloat(String(a.usdcSize)) : undefined,
    timestamp: Number(a.timestamp) || 0,
  }));
}

/** Compute our bet: copyPercent of target's bet, capped at maxBetUsd, min minBetUsd */
export function computeBetSizeFromTarget(
  targetBetUsd: number,
  copyPercent: number,
  maxBetUsd: number,
  minBetUsd: number
): number {
  const amount = (targetBetUsd * copyPercent) / 100;
  const capped = Math.min(amount, maxBetUsd);
  return capped >= minBetUsd ? capped : 0;
}

export interface CopiedTrade {
  title: string;
  outcome: string;
  side: string;
  amountUsd: number;
  price: number;
  asset: string;
  timestamp: number;
}

export interface CopyTradeResult {
  copied: number;
  failed: number;
  error?: string;
  lastTimestamp?: number;
  copiedKeys: string[];
  copiedTrades: CopiedTrade[];
}

export async function runCopyTrade(
  privateKey: string,
  myAddress: string,
  targetAddress: string,
  signatureType: number,
  config: { copyPercent: number; maxBetUsd: number; minBetUsd: number; stopLossBalance: number },
  state: { lastTimestamp: number; copiedKeys: string[] }
): Promise<CopyTradeResult> {
  const result: CopyTradeResult = { copied: 0, failed: 0, copiedKeys: [], copiedTrades: [] };

  const signer = new Wallet(privateKey);
  const rawClient = new ClobClient(CLOB_HOST, CHAIN_ID, signer);
  const creds = await rawClient.createOrDeriveApiKey();
  const client = new ClobClient(
    CLOB_HOST,
    CHAIN_ID,
    signer,
    creds,
    signatureType,
    myAddress
  );

  const cashBalance = await getCashBalance(myAddress);
  if (cashBalance < 1) {
    result.error = "Low balance";
    return result;
  }
  if (config.stopLossBalance > 0 && cashBalance < config.stopLossBalance) {
    result.error = `Stop-loss: balance $${cashBalance.toFixed(2)} below threshold $${config.stopLossBalance}`;
    return result;
  }

  const activities = await getTargetActivity(targetAddress, 50);
  let lastTimestamp = state.lastTimestamp;
  const copiedSet = new Set(state.copiedKeys);
  const isFirstRun = lastTimestamp === 0 && copiedSet.size === 0;

  // On first run, only copy trades from the last 5 minutes (avoid ancient history)
  const nowSec = Math.floor(Date.now() / 1000);
  const fiveMinAgo = nowSec - 300;

  for (const act of activities) {
    if (act.type !== "TRADE") continue;
    const ts = act.timestamp;
    if (ts <= lastTimestamp) continue;
    if (isFirstRun && ts < fiveMinAgo) continue; // Skip trades older than 5 min on first run

    const txHash = act.transactionHash ?? "";
    const asset = act.asset ?? "";
    const sideStr = (act.side ?? "BUY").toUpperCase();
    const price = act.price;

    if (!asset || price <= 0) continue;

    const key = `${txHash}|${asset}|${sideStr}`;
    if (copiedSet.has(key)) continue;

    // Target's bet in USD: use usdcSize if available, else size * price
    const targetBetUsd = act.usdcSize ?? (act.size ?? 0) * price;
    const betUsd = computeBetSizeFromTarget(
      targetBetUsd,
      config.copyPercent,
      config.maxBetUsd,
      config.minBetUsd
    );
    if (betUsd < config.minBetUsd) continue;

    const side = sideStr === "BUY" ? Side.BUY : Side.SELL;

    try {
      // Omit price for BUY so client uses current order book price (improves FOK fill rate)
      const orderParams = {
        tokenID: asset,
        amount: betUsd,
        side,
        orderType: OrderType.FOK as const,
        ...(side === Side.SELL && { price }),
      };
      const resp = await client.createAndPostMarketOrder(
        orderParams,
        undefined,
        OrderType.FOK
      );

      if (resp?.success) {
        copiedSet.add(key);
        lastTimestamp = Math.max(lastTimestamp ?? 0, ts);
        result.copied++;
        result.copiedTrades.push({
          title: act.title ?? "Unknown",
          outcome: act.outcome ?? (sideStr === "BUY" ? "Yes" : "No"),
          side: sideStr,
          amountUsd: betUsd,
          price,
          asset,
          timestamp: Date.now(),
        });
      } else {
        result.failed++;
        const errMsg =
          resp?.errorMsg ??
          (typeof resp?.error === "string" ? resp.error : null) ??
          (resp?.error && typeof resp.error === "object" ? JSON.stringify(resp.error).slice(0, 100) : null) ??
          resp?.message ??
          (resp?.status ? `HTTP ${resp.status}` : null) ??
          "Order rejected";
        const next = result.error ? `${result.error}; ${errMsg}` : errMsg;
        result.error = next.length > 500 ? `${next.slice(0, 497)}...` : next;
        console.error("Copy trade failed:", errMsg, act.title);
      }
    } catch (e) {
      result.failed++;
      const errStr = e instanceof Error ? e.message : String(e);
      const next = result.error ? `${result.error}; ${errStr}` : errStr;
      result.error = next.length > 500 ? `${next.slice(0, 497)}...` : next;
      console.error("Copy trade error:", e);
    }
  }

  // Only advance lastTimestamp when we successfully copy—never skip trades we failed to copy
  result.lastTimestamp = lastTimestamp;
  result.copiedKeys = Array.from(copiedSet);
  return result;
}

export async function sellPosition(
  privateKey: string,
  myAddress: string,
  signatureType: number,
  asset: string,
  sizeShares: number,
  price: number
): Promise<{ success: boolean; error?: string }> {
  const signer = new Wallet(privateKey);
  const rawClient = new ClobClient(CLOB_HOST, CHAIN_ID, signer);
  const creds = await rawClient.createOrDeriveApiKey();
  const client = new ClobClient(
    CLOB_HOST,
    CHAIN_ID,
    signer,
    creds,
    signatureType,
    myAddress
  );

  try {
    const resp = await client.createAndPostMarketOrder(
      {
        tokenID: asset,
        amount: sizeShares,
        side: Side.SELL,
        price: Math.max(0.01, Math.min(0.99, price)),
        orderType: OrderType.FOK,
      },
      undefined,
      OrderType.FOK
    );
    return { success: !!resp?.success, error: resp?.errorMsg };
  } catch (e) {
    const err = e instanceof Error ? e.message : String(e);
    return { success: false, error: err };
  }
}
