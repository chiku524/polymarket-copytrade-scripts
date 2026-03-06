# Polymarket Paired Strategy Trader

Run your own BTC/ETH Up-Down paired strategy on Polymarket with **Off / Paper / Live** modes, wallet budget caps, and paper analytics before going live.

## Web UI (Fly.io)

A Next.js app provides a control UI to set trading mode (**Off / Paper / Live**), adjust sizing, cap wallet usage per run, and run manually.

### Deploy to Fly.io (two apps: web + worker)

Use two Fly.io apps from this repo:
- **polymarket-trader** (web): Next.js UI + API routes
- **polymarket-trader-worker**: persistent strategy worker loop

#### 1) Create apps and deploy

**Web app:**
```bash
fly launch --config fly.toml  # or fly deploy --config fly.toml
```

**Worker app (after web is live):**
```bash
fly launch --config fly.worker.toml  # or fly deploy --config fly.worker.toml
```

#### 2) Ensure Mumbai region and egress (required for Polymarket)

Polymarket blocks US/EU-based IPs. You need both:

**A) Machines in Mumbai (`bom`)** – run the migration script if you see US/EU regions:

```bash
bash scripts/fly-migrate-to-bom.sh
```

**B) Static egress IP in Mumbai (`bom`)** – Fly’s default IPv4 egress uses shared NAT and may still route through blocked locations. Allocate Mumbai egress so outbound requests to Polymarket use an allowed IP:

```bash
fly ips allocate-egress -a polymarket-trader -r bom
```

This costs about $3.60/mo for IPv4. After allocation, existing machines may take a short time to use the new IP; redeploy if needed:

```bash
fly deploy -a polymarket-trader -c fly.toml --remote-only --depot=false --primary-region bom -y
```

Verify with **Diagnostics & debug**: geoblock should show `blocked: false`, `country` = IN. Use the Fly URL, not localhost. Redis and the worker do not affect geoblock; only the web app’s outbound IP matters.

#### 3) Add Redis

- Use [Fly.io Redis](https://fly.io/docs/reference/redis/) or an external Redis (Upstash, etc.).
- Set `REDIS_URL` on the **web app** (`polymarket-trader`). The worker does not use Redis.

#### 4) Set secrets (both apps)

```bash
fly secrets set PRIVATE_KEY=... MY_ADDRESS=... SIGNATURE_TYPE=1 CRON_SECRET=... REDIS_URL=...
```

Optional: `POLY_BUILDER_API_KEY`, `POLY_BUILDER_SECRET`, `POLY_BUILDER_PASSPHRASE`, `POLYGON_RPC_URL`, `CLAIM_EVERY_N_RUNS`, `ALERT_WEBHOOK_URL`, `ALERT_WEBHOOK_TOKEN`.

**Worker app only:** `APP_BASE_URL` (e.g. `https://polymarket-trader.fly.dev`). It’s pre-set in `fly.worker.toml` for the default app name; adjust if your web app URL differs.

#### 5) GitHub Actions (optional)

`.github/workflows/fly-deploy.yml` deploys the web app and worker on push to `main`.

**Token setup (required for CI deploys):** You need an org-scoped token (or two app-scoped tokens) because the workflow deploys two apps.

- **Option A – Org-scoped (recommended):** Deploy both apps with one token:
  ```bash
  fly tokens create org --name "github-actions" -x 999999h
  ```
  Copy the full output (including `FlyV1` and space). Add it as repo secret `FLY_API_TOKEN` in GitHub: **Settings → Secrets and variables → Actions → New repository secret**.

- **Option B – App-scoped:** Create one token per app, then use two secrets (`FLY_API_TOKEN` for web, `FLY_API_TOKEN_WORKER` for worker) and update the workflow to use the appropriate token per job.

If you get `Error: unauthorized`, verify: (1) the secret exists and is spelled `FLY_API_TOKEN`, (2) the token was copied in full, (3) you used an org-scoped token (or matching app-scoped tokens).

#### 6) Claiming winnings

Resolved positions must be claimed to move winnings into available cash:
- app auto-claims every `CLAIM_EVERY_N_RUNS` (default 10)
- or use **Claim now** in UI / `POST /api/claim-now`

### Local dev

```bash
npm install
npm run dev
```

Requires Redis (set `REDIS_URL` locally).

### Persistent worker (recommended over cron)

Instead of cron, you can run an always-on worker that continuously triggers the strategy run endpoint.

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

## Is This Doable?

**Yes.** The public Polymarket API supports everything needed:

| Need | API | Endpoint |
|------|-----|----------|
| Your cash balance | Data API | `GET /v1/accounting/snapshot` |
| Place orders | CLOB API | `POST /order` (auth required) |

Docs: [Polymarket Developer Quickstart](https://docs.polymarket.com/quickstart/overview)

## Configuration Reference

| Variable | Description |
|----------|-------------|
| `MY_ADDRESS` | Your Polymarket proxy/funder address |
| `CRON_SECRET` | Required for worker-to-web auth (random string) |

### UI Controls (Web App)

- **Mode**: `Off` (paused), `Paper` (simulate only), `Live` (real orders)
- **Paper high-data preset**: one-click profile for dense paper sampling (`Paper` mode, 0.1c edge floor, max markets/run, focused on BTC/ETH 5m+15m)
- **Wallet usage % / run**: caps how much balance can be spent each run in Paper/Live
- **Coins**: enable/disable `BTC` and `ETH` independently
- **Cadence filters**: enable/disable `5m`, `15m`, and `Hourly` Up/Down markets
- **Cadence min-edge thresholds**: tune different edge requirements for `5m`, `15m`, and `Hourly`
- **Edge quality buffers**: configure estimated fee (`bps`) and slippage (`¢/leg`) so execution uses net edge, not just raw edge
- **Cadence-aware ranking**: execution priority favors stronger net-edge surplus with cadence-specific freshness weighting
- **Market freshness guards**: stale signals and large per-leg timestamp skews are rejected before execution
- **Diagnostics trend (last N runs)**: view rolling execution/rejection trends and Phase 2 coin/cadence mix
- **Auto-tune suggestions**: one-click suggested cadence edge updates from trend diagnostics
- **Live safety guardrails**: configurable `max unresolved imbalances/run`, `unwind slippage`, and `unwind share buffer` for one-leg recovery
- **Daily live risk caps**: configurable max daily notional and max daily drawdown hard-stops
- **Safety latch preflight**: attempts to unwind prior unresolved exposure before allowing new live entries
- **Alert webhook (optional)**: set `ALERT_WEBHOOK_URL` (and optional `ALERT_WEBHOOK_TOKEN`) for critical safety notifications
