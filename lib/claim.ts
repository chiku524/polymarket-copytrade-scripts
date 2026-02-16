import { Wallet, Contract, providers } from "ethers";
import { createWalletClient, http, type Hex } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";
import { encodeFunctionData } from "viem";
import { RelayClient, RelayerTxType } from "@polymarket/builder-relayer-client";
import { BuilderConfig } from "@polymarket/builder-signing-sdk";
import { getPositions } from "@/lib/polymarket";

const CHAIN_ID = 137;
const CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const RELAYER_URL = "https://relayer-v2.polymarket.com/";
const POLYGON_RPC =
  process.env.POLYGON_RPC_URL ?? "https://polygon-rpc.com";

const CTF_REDEEM_ABI = [
  {
    inputs: [
      { name: "collateralToken", type: "address" },
      { name: "parentCollectionId", type: "bytes32" },
      { name: "conditionId", type: "bytes32" },
      { name: "indexSets", type: "uint256[]" },
    ],
    name: "redeemPositions",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function" as const,
  },
] as const;

export interface ClaimResult {
  claimed: number;
  failed: number;
  errors: string[];
  txHashes: string[];
}

function normalizeConditionId(id: string): string {
  const hex = id.startsWith("0x") ? id.slice(2) : id;
  return "0x" + hex.padStart(64, "0").slice(-64);
}

/**
 * Claim winnings via Polymarket relayer (proxy wallet). Requires Builder API keys.
 */
async function claimViaRelayer(
  privateKey: string,
  conditionIds: string[],
  result: ClaimResult
): Promise<void> {
  const key = process.env.POLY_BUILDER_API_KEY ?? process.env.BUILDER_API_KEY;
  const secret = process.env.POLY_BUILDER_SECRET ?? process.env.BUILDER_SECRET;
  const passphrase =
    process.env.POLY_BUILDER_PASSPHRASE ?? process.env.BUILDER_PASSPHRASE;
  if (!key || !secret || !passphrase) return;

  const account = privateKeyToAccount(privateKey as Hex);
  const wallet = createWalletClient({
    account,
    chain: polygon,
    transport: http(POLYGON_RPC),
  });
  const builderConfig = new BuilderConfig({
    localBuilderCreds: { key, secret, passphrase },
  });
  const relayClient = new RelayClient(
    RELAYER_URL,
    CHAIN_ID,
    wallet as never,
    builderConfig,
    RelayerTxType.PROXY
  );

  const zeroHash =
    "0x0000000000000000000000000000000000000000000000000000000000000000" as const;

  for (const conditionId of conditionIds) {
    try {
      const data = encodeFunctionData({
        abi: CTF_REDEEM_ABI,
        functionName: "redeemPositions",
        args: [USDC_E as Hex, zeroHash, normalizeConditionId(conditionId) as Hex, [BigInt(1), BigInt(2)]],
      });
      const response = await relayClient.execute(
        [{ to: CTF_ADDRESS as Hex, data, value: "0" }],
        "Redeem winnings"
      );
      const waitResult = await response.wait();
      if (waitResult?.transactionHash) {
        result.claimed++;
        result.txHashes.push(waitResult.transactionHash);
      }
    } catch (e) {
      result.failed++;
      const msg = e instanceof Error ? e.message : String(e);
      result.errors.push(`${conditionId.slice(0, 10)}…: ${msg.slice(0, 80)}`);
      console.error("Claim (relayer) failed for condition", conditionId, e);
    }
  }
}

/**
 * Claim winnings via direct contract call (EOA must hold the tokens).
 */
async function claimViaDirect(
  privateKey: string,
  conditionIds: string[],
  result: ClaimResult
): Promise<void> {
  const provider = new providers.JsonRpcProvider(POLYGON_RPC);
  const signer = new Wallet(privateKey, provider);
  const ctf = new Contract(
    CTF_ADDRESS,
    [
      "function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets) external",
    ],
    signer
  );
  const parentCollectionId =
    "0x0000000000000000000000000000000000000000000000000000000000000000";
  const indexSets = [1, 2];

  for (const conditionId of conditionIds) {
    try {
      const tx = await ctf.redeemPositions(
        USDC_E,
        parentCollectionId,
        normalizeConditionId(conditionId),
        indexSets
      );
      const receipt = await tx.wait();
      if (receipt?.transactionHash) {
        result.claimed++;
        result.txHashes.push(receipt.transactionHash);
      }
    } catch (e) {
      result.failed++;
      const msg = e instanceof Error ? e.message : String(e);
      result.errors.push(`${conditionId.slice(0, 10)}…: ${msg.slice(0, 80)}`);
      console.error("Claim (direct) failed for condition", conditionId, e);
    }
  }
}

/**
 * Fetch redeemable positions, group by conditionId, and claim winnings.
 * Uses Builder Relayer (proxy wallet) when POLY_BUILDER_API_KEY + secret + passphrase are set;
 * otherwise tries direct CTF call (works only if EOA holds the tokens).
 */
export async function claimWinnings(
  privateKey: string,
  myAddress: string
): Promise<ClaimResult> {
  const result: ClaimResult = { claimed: 0, failed: 0, errors: [], txHashes: [] };
  const positions = await getPositions(myAddress, 200);
  const redeemable = positions.filter((p) => p.redeemable && p.size > 0);
  if (redeemable.length === 0) {
    return result;
  }

  const conditionIds = Array.from(new Set(redeemable.map((p) => p.conditionId))).filter(
    (id): id is string => typeof id === "string" && id.length > 0
  );

  const useRelayer =
    !!(process.env.POLY_BUILDER_API_KEY ?? process.env.BUILDER_API_KEY) &&
    !!(process.env.POLY_BUILDER_SECRET ?? process.env.BUILDER_SECRET) &&
    !!(process.env.POLY_BUILDER_PASSPHRASE ?? process.env.BUILDER_PASSPHRASE);

  if (useRelayer) {
    await claimViaRelayer(privateKey, conditionIds, result);
  } else {
    await claimViaDirect(privateKey, conditionIds, result);
  }

  return result;
}
