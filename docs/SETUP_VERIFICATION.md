# Setup Verification Guide

End-to-end checklist to ensure the Polymarket copy-trade system is configured correctly.

---

## 1. Redis Region (Stockholm vs Mumbai)

**Q: Upstash Redis is in Stockholm (arn); web app and worker are in Mumbai (bom). Is that an issue?**

**A: No.** Redis region is independent of Polymarket geoblock:

| Concern | Verdict |
|--------|---------|
| **Geoblock** | Only the web app’s **outbound IP** to polymarket.com matters. That’s determined by Fly egress in Mumbai. Redis is not involved. |
| **Latency** | Mumbai → Stockholm adds ~150–250 ms per round trip. A run does ~5–15 Redis ops, so ~1–3 s of Redis latency vs ~30–90 s total. Acceptable. |
| **Connectivity** | Upstash is a global service; Mumbai → Stockholm is well-connected. |

**Recommendation:** Keep Redis in Stockholm if that’s where it’s provisioned. Moving it to Mumbai would reduce latency slightly but is optional.

---

## 2. Redis URL Format

**Correct format:**
```
redis://default:PASSWORD@fly-polymarket-trader-redis.upstash.io:6379
```
or, if TLS is required:
```
rediss://default:PASSWORD@fly-polymarket-trader-redis.upstash.io:6379
```

**Notes:**
- `redis://` = non-TLS, `rediss://` = TLS (note the double `s`).
- Upstash usually enforces TLS. If you see connection errors with `redis://`, try `rediss://`.
- Never use `http://` for Redis URLs.

**Where to set:** As Fly secret `REDIS_URL` on the **web app** (`polymarket-trader`). The worker does not use Redis.

---

## 3. Full Architecture

```
[Worker (Mumbai)]  --POST /api/run-now + x-worker-shared-secret-->  [Web app (Mumbai)]
                                                                         |
                                                                         v
                                                               [Polymarket API] (outbound from Mumbai egress IP)
                                                                         |
                                                                         v
                                                               [Redis (Stockholm)]  <-- config, state, paper stats, etc.
```

- **Worker** → only HTTP calls to the web app; does not touch Redis.
- **Web app** → reads/writes Redis, calls Polymarket, runs strategy.

---

## 4. Environment Variables Checklist

### Web app (`polymarket-trader`)

| Variable | Required | Notes |
|----------|----------|-------|
| `REDIS_URL` | Yes | Full Redis URL (e.g. `redis://` or `rediss://`) |
| `CRON_SECRET` | Yes | Random secret; worker sends `Authorization: Bearer CRON_SECRET` |
| `PRIVATE_KEY` | For live mode | Wallet private key |
| `MY_ADDRESS` | Yes | Polymarket proxy/funder address |
| `SIGNATURE_TYPE` | Yes | Usually `1` |
| `POLY_BUILDER_*` | For claiming | API keys from Polymarket profile |

### Worker (`polymarket-trader-worker`)

| Variable | Required | Notes |
|----------|----------|-------|
| `APP_BASE_URL` | Yes | e.g. `https://polymarket-trader.fly.dev` (set in fly.worker.toml) |
| `WORKER_SHARED_SECRET` | Recommended | Shared secret header for non-browser `/api/run-now` calls |
| `CRON_SECRET` | Optional | Only required if you point worker at `/api/copy-trade` |
| (no REDIS_URL) | — | Worker does not use Redis |

---

## 5. Verification Commands

### Check secrets (web app)
```bash
fly secrets list -a polymarket-trader
```
Must include: `REDIS_URL`, `CRON_SECRET`, `MY_ADDRESS`, `SIGNATURE_TYPE`; `PRIVATE_KEY` for live.

### Check secrets (worker)
```bash
fly secrets list -a polymarket-trader-worker
```
Must include: `WORKER_SHARED_SECRET` (recommended). `APP_BASE_URL` is in fly.worker.toml `[env]`.

### Check regions
```bash
fly status -a polymarket-trader
fly status -a polymarket-trader-worker
```
Both should show `bom` (Mumbai).

### Test geoblock (use Fly URL, not localhost)
```bash
curl https://polymarket-trader.fly.dev/api/debug
```
Expect `geoblock.blocked: false`, `geoblock.country: "IN"`.

### Test status (includes Redis-backed data)
```bash
curl https://polymarket-trader.fly.dev/api/status
```
If Redis is working: `config`, `state`, `paperStats`, etc. will be populated. If Redis fails: 500 or partial/missing data.

### Test worker endpoint (manual run)
```bash
curl -X POST \
  -H "x-worker-shared-secret: YOUR_WORKER_SHARED_SECRET" \
  https://polymarket-trader.fly.dev/api/run-now
```

---

## 6. Paper Stats Flow

1. Worker calls `POST /api/run-now` with `x-worker-shared-secret`.
2. Web app runs strategy; in paper mode calls `recordPaperRun()`.
3. `recordPaperRun()` writes to Redis key `paper-stats` via `lib/kv.ts`.
4. User opens UI → frontend calls `GET /api/status`.
5. Web app reads `getPaperStats()` from Redis.
6. Frontend displays Paper analytics.

**If Paper analytics are empty:**
- `REDIS_URL` not set on web app → in-memory fallback; each Fly instance has its own memory.
- Check logs for: `REDIS_URL not configured; using in-memory KV fallback`.
- Ensure `REDIS_URL` is set as a secret on `polymarket-trader`.

---

## 7. Common Issues

| Symptom | Likely cause |
|---------|--------------|
| Paper stats empty | `REDIS_URL` not set on web app; in-memory KV used per instance |
| 401 Unauthorized on run-now (worker) | `WORKER_SHARED_SECRET` missing/mismatch between worker and web app |
| 401 Unauthorized on copy-trade | Worker is still pointing to `/api/copy-trade` without matching `CRON_SECRET` |
| Worker "skipped" every run | Mode is off, or safety latch active |
| Geoblock blocked | Web app egress IP in restricted region; ensure Mumbai egress allocated |
| Redis connection errors | Wrong URL format; try `rediss://` instead of `redis://` if TLS required |

---

## 8. Quick Sanity Check

1. Deploy both apps to Mumbai.
2. Set `REDIS_URL` on web app and set `WORKER_SHARED_SECRET` on both web app + worker.
3. Open `https://polymarket-trader.fly.dev` and set mode to Paper.
4. Wait 1–2 minutes for worker runs.
5. Check Diagnostics & debug: `lastRunAt` should update, geoblock `blocked: false`, `country: IN`.
6. Check Paper analytics tab: stats should appear after paper runs.
