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

export interface CopiedTrade {
  title: string;
  outcome: string;
  side: string;
  amountUsd: number;
  price: number;
  asset: string;
  timestamp: number;
  conditionId?: string;
  slug?: string;
  coin?: "BTC" | "ETH";
  cadence?: "5m" | "15m" | "hourly" | "other";
  edgeCentsAtEntry?: number;
  netEdgeCentsAtEntry?: number;
  dynamicSizingScalePct?: number;
  edgeBoostScalePct?: number;
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
