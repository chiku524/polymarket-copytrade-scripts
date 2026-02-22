# Polymarket Paired Strategy Trader

Run your own BTC/ETH Up-Down paired strategy on Polymarket with **Off / Paper / Live** modes, wallet budget caps, and paper analytics before going live.

## Web UI (Vercel)

A Next.js app provides a control UI to set trading mode (**Off / Paper / Live**), adjust sizing, cap wallet usage per run, and run manually.

### Deploy to Vercel

1. **Connect repo** on [vercel.com](https://vercel.com) and deploy.

2. **Add Redis** (Storage → Redis, or Marketplace → Upstash Redis):
   - Create a Redis database and link it to your project
   - Env vars `KV_REST_API_URL` and `KV_REST_API_TOKEN` auto-populate (required for build)

3. **Environment variables** (Settings → Environment Variables):
   - `PRIVATE_KEY` – Your wallet private key
   - `MY_ADDRESS` – `0x370e81c93aa113274321339e69049187cce03bb9`
   - `TARGET_ADDRESS` – optional, used only for target analysis/debug tooling
   - `SIGNATURE_TYPE` – `1` (Email/Magic) or `2` (Browser wallet)
   - `CRON_SECRET` – Any random string (e.g. `openssl rand -hex 32`) to secure the cron job

4. **Cron** runs every minute when scheduled. Set mode to **Live** in the UI to place real orders, or **Paper** to simulate.

5. **Claiming winnings** – Resolved positions must be “claimed” to move winnings to your cash balance. The app:
   - Runs **claim automatically every 10 strategy runs** (configurable via `CLAIM_EVERY_N_RUNS`).
   - Or use the **Claim now** button in the UI (or `POST /api/claim-now`).
   - For **Polymarket proxy wallets** (default), set Builder API keys so the relayer can execute the claim from your proxy:
     - `POLY_BUILDER_API_KEY`, `POLY_BUILDER_SECRET`, `POLY_BUILDER_PASSPHRASE` (or `BUILDER_API_KEY`, `BUILDER_SECRET`, `BUILDER_PASSPHRASE`).
   - Optional: `POLYGON_RPC_URL` (default: public Polygon RPC), `CLAIM_EVERY_N_RUNS` (default: 10).

### Local dev

```bash
npm install
npm run dev
```

Requires Vercel KV. Use `vercel link` and `vercel env pull` to pull env vars locally.

### Persistent worker (recommended over cron)

Instead of cron, you can run an always-on worker that continuously triggers `/api/copy-trade`.

1. Set env vars for the worker process:
   - `APP_BASE_URL` (e.g. `https://your-app-domain.com`)
   - `CRON_SECRET` (must match your app env)
   - optional: `WORKER_INTERVAL_MS` (default `15000`)
   - optional: `WORKER_REQUEST_TIMEOUT_MS` (default `70000`)

2. Start the worker:

```bash
npm run worker
```

3. Deploy pattern (production):
   - Service A: web app (`next start`)
   - Service B: worker (`npm run worker`)
   - Both share the same Redis + wallet env vars.

Control behavior from UI:
- **Off** = pause (worker keeps running but places no new orders)
- **Paper** = simulate only
- **Live** = real orders
- Paper analytics can be viewed in UI and via `GET /api/paper-stats` (reset with `DELETE /api/paper-stats`).

To avoid duplicate triggers, run **either** cron **or** worker (not both).

---

## Python Script (Standalone)

## Is This Doable?

**Yes.** The public Polymarket API supports everything needed:

| Need | API | Endpoint |
|------|-----|----------|
| Target user's trades | Data API | `GET /activity` |
| Your cash balance | Data API | `GET /v1/accounting/snapshot` |
| Place orders | CLOB API | `POST /order` (auth required) |

Docs: [Polymarket Developer Quickstart](https://docs.polymarket.com/quickstart/overview)

## Setup

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Create `.env`**

   ```bash
   cp config.example.env .env
   ```

3. **Edit `.env`**

   - `PRIVATE_KEY` – Your wallet private key (from [reveal.polymarket.com](https://reveal.polymarket.com) or your wallet)
   - `MY_ADDRESS` – Your Polymarket proxy/funder address (profile dropdown)
   - `SIGNATURE_TYPE` – `0` EOA, `1` Email/Magic, `2` Browser wallet
   - `TARGET_ADDRESS` – Address to copy (default: gabagool22’s proxy)

## Run

```bash
python copy_trader.py
```

The script will:

1. Sync to the target’s latest trades (no historical copies on first run)
2. Poll every 15 seconds for new trades
3. For each new trade, place a market order sized at 5–10% of your cash balance based on odds
4. Use FOK (Fill-Or-Kill) orders for immediate execution

## Position Sizing

- **5%** at price 0 (long shot)
- **10%** at price 1 (favorite)
- Linear interpolation in between

Tune via `MIN_PERCENT` and `MAX_PERCENT` in `.env`.

## Limitations

- **Latency**: Data API is on-chain. There is a delay (typically ~30–60 seconds) before trades appear.
- **Geographic restrictions**: Polymarket enforces geo-blocking; check [geoblocking docs](https://docs.polymarket.com/developers/CLOB/geoblock).
- **Token allowances**: EOA/MetaMask users must set allowances before trading. See the [py-clob-client README](https://github.com/Polymarket/py-clob-client#important-token-allowances-for-metamaskeoa-users).

## Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `MY_ADDRESS` | `0x370e...` | Your Polymarket wallet |
| `TARGET_ADDRESS` | `0x6031...` | gabagool22’s wallet |
| `MIN_PERCENT` | `0.05` | Min % of balance per bet |
| `MAX_PERCENT` | `0.10` | Max % of balance per bet |
| `POLL_INTERVAL` | `15` | Seconds between checks |
| `MIN_BET_USD` | `1.0` | Minimum bet size (USDC) |
| `CRON_SECRET` | — | Required for Vercel cron (random string) |

### UI Controls (Web App)

- **Mode**: `Off` (paused), `Paper` (simulate only), `Live` (real orders)
- **Wallet usage % / run**: caps how much balance can be spent each run in Paper/Live
