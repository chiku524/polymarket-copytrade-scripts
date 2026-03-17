const DATA_API = "https://data-api.polymarket.com";
const CLOB_HOST = "https://clob.polymarket.com";
const MARKET_STATUS_CACHE_TTL_MS = 15_000;
const marketStatusCache = new Map<string, { fetchedAt: number; market: ClobMarketStatus | null }>();

export interface Position {
  asset: string;
  conditionId: string;
  size: number;
  avgPrice: number;
  initialValue: number;
  currentValue: number;
  cashPnl: number;
  percentPnl: number;
  curPrice: number;
  title: string;
  slug: string;
  eventSlug?: string;
  icon?: string;
  outcome: string;
  oppositeOutcome: string;
  redeemable: boolean;
  mergeable: boolean;
  endDate?: string;
}

export interface ClosedPosition {
  asset: string;
  conditionId: string;
  avgPrice: number;
  totalBought: number;
  realizedPnl: number;
  curPrice: number;
  timestamp: number;
  title: string;
  slug: string;
  icon?: string;
  outcome: string;
  eventSlug?: string;
}

export interface ClobMarketStatusToken {
  token_id: string;
  outcome?: string;
  price?: number | string;
  winner?: boolean;
}

export interface ClobMarketStatus {
  condition_id: string;
  closed?: boolean;
  active?: boolean;
  accepting_orders?: boolean;
  market_slug?: string;
  question?: string;
  tokens?: ClobMarketStatusToken[];
}

export async function getPositions(address: string, limit = 100): Promise<Position[]> {
  const params = new URLSearchParams({
    user: address,
    limit: String(limit),
    sortBy: "TOKENS",
    sortDirection: "DESC",
  });
  const res = await fetch(`${DATA_API}/positions?${params}`);
  if (!res.ok) throw new Error(`Positions failed: ${res.status}`);
  const data = (await res.json()) as Position[];
  return Array.isArray(data) ? data : [];
}

export async function getClosedPositions(address: string, limit = 50): Promise<Position[]> {
  const params = new URLSearchParams({
    user: address,
    limit: String(limit),
    sortBy: "TIMESTAMP",
    sortDirection: "DESC",
  });
  const res = await fetch(`${DATA_API}/closed-positions?${params}`);
  if (!res.ok) throw new Error(`Closed positions failed: ${res.status}`);
  const data = (await res.json()) as ClosedPosition[];
  return (Array.isArray(data) ? data : []).map((c) => ({
    asset: c.asset,
    conditionId: c.conditionId,
    title: c.title,
    outcome: c.outcome,
    size: c.totalBought,
    avgPrice: c.avgPrice,
    initialValue: c.totalBought * c.avgPrice,
    currentValue: 0,
    cashPnl: c.realizedPnl,
    percentPnl: c.avgPrice > 0 ? (c.realizedPnl / (c.totalBought * c.avgPrice)) * 100 : 0,
    curPrice: c.curPrice,
    icon: c.icon,
    slug: c.slug,
    eventSlug: c.eventSlug ?? c.slug,
    redeemable: true,
    oppositeOutcome: "",
    mergeable: false,
  }));
}

export async function getMarketsByConditionIds(
  conditionIds: string[]
): Promise<Map<string, ClobMarketStatus>> {
  const now = Date.now();
  const uniqueIds = Array.from(new Set(conditionIds.map((id) => String(id ?? "").trim()).filter(Boolean)));
  const result = new Map<string, ClobMarketStatus>();
  if (uniqueIds.length === 0) return result;

  const idsToFetch: string[] = [];
  for (const conditionId of uniqueIds) {
    const cached = marketStatusCache.get(conditionId);
    if (cached && now - cached.fetchedAt <= MARKET_STATUS_CACHE_TTL_MS) {
      if (cached.market) result.set(conditionId, cached.market);
      continue;
    }
    idsToFetch.push(conditionId);
  }

  const BATCH_SIZE = 20;
  for (let i = 0; i < idsToFetch.length; i += BATCH_SIZE) {
    const batch = idsToFetch.slice(i, i + BATCH_SIZE);
    const query = encodeURIComponent(batch.join(","));
    const res = await fetch(`${CLOB_HOST}/markets?condition_ids=${query}`, { cache: "no-store" });
    if (!res.ok) throw new Error(`Markets lookup failed: ${res.status}`);
    const payload = (await res.json()) as { data?: unknown } | unknown[];
    const markets = Array.isArray(payload)
      ? payload
      : payload && typeof payload === "object" && Array.isArray((payload as { data?: unknown }).data)
        ? ((payload as { data: unknown[] }).data ?? [])
        : [];
    const seen = new Set<string>();
    for (const item of markets) {
      if (!item || typeof item !== "object") continue;
      const raw = item as Record<string, unknown>;
      const conditionId = String(raw.condition_id ?? "").trim();
      if (!conditionId || !batch.includes(conditionId)) continue;
      const tokens: ClobMarketStatusToken[] = Array.isArray(raw.tokens)
        ? raw.tokens
            .map((token): ClobMarketStatusToken | null => {
              if (!token || typeof token !== "object") return null;
              const t = token as Record<string, unknown>;
              const tokenId = String(t.token_id ?? "").trim();
              if (!tokenId) return null;
              return {
                token_id: tokenId,
                outcome: typeof t.outcome === "string" ? t.outcome : undefined,
                price: typeof t.price === "number" || typeof t.price === "string" ? t.price : undefined,
                winner: typeof t.winner === "boolean" ? t.winner : undefined,
              };
            })
            .filter((token): token is ClobMarketStatusToken => Boolean(token))
        : [];
      const market: ClobMarketStatus = {
        condition_id: conditionId,
        closed: raw.closed === true,
        active: raw.active !== false,
        accepting_orders: raw.accepting_orders === true,
        market_slug: typeof raw.market_slug === "string" ? raw.market_slug : undefined,
        question: typeof raw.question === "string" ? raw.question : undefined,
        tokens,
      };
      result.set(conditionId, market);
      marketStatusCache.set(conditionId, { fetchedAt: now, market });
      seen.add(conditionId);
    }
    for (const conditionId of batch) {
      if (seen.has(conditionId)) continue;
      marketStatusCache.set(conditionId, { fetchedAt: now, market: null });
    }
  }
  return result;
}
