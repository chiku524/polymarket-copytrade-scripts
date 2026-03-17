import { NextResponse } from "next/server";
import {
  getConfig,
  setConfig,
  getState,
  setState,
  appendActivity,
  acquireRunLock,
  releaseRunLock,
  recordPaperRun,
  recordPaperTrades,
  recordStrategyDiagnostics,
} from "@/lib/kv";
import { runPairedStrategy } from "@/lib/paired-strategy";
import { getCashBalance } from "@/lib/copy-trade";
import { sendAlert } from "@/lib/alerts";
import {
  applyDailyLiveRun,
  attemptResolveSafetyLatch,
  evaluateDailyRiskCaps,
  initDailyRiskState,
  shouldSendLatchAlert,
} from "@/lib/live-safety";

const PRIVATE_KEY = process.env.PRIVATE_KEY;
const MY_ADDRESS = process.env.MY_ADDRESS ?? "0x370e81c93aa113274321339e69049187cce03bb9";
const SIGNATURE_TYPE = parseInt(process.env.SIGNATURE_TYPE ?? "1", 10);
const LATCH_ALERT_COOLDOWN_MS = 15 * 60 * 1000;

export const maxDuration = 60;

async function enforceAutoStopTimer() {
  const config = await getConfig();
  const autoStopAt = Number(config.autoStopAt ?? 0);
  if (autoStopAt <= 0 || Date.now() < autoStopAt) {
    return { config, expired: false as const };
  }

  // Timer elapsed: force strategy off before this run.
  const updatedConfig = await setConfig({ mode: "off", enabled: false, autoStopAt: 0 });
  const message = `Run timer expired at ${new Date(autoStopAt).toISOString()}; mode set to off`;
  return { config: updatedConfig, expired: true as const, message };
}

/** GET returns instructions - use POST from the Run now button */
export async function GET() {
  return NextResponse.json(
    { message: "Use POST to trigger strategy run (Run now button)" },
    { status: 200 }
  );
}

/**
 * Manual trigger - no auth required (same-origin only in production).
 * Use for "Run now" button in the UI.
 */
export async function POST() {
  const lockToken = await acquireRunLock(120);
  if (!lockToken) {
    return NextResponse.json({ ok: true, skipped: true, reason: "busy" });
  }

  try {
    const timerCheck = await enforceAutoStopTimer();
    const config = timerCheck.config;
    if (timerCheck.expired) {
      await setState({ lastRunAt: Date.now(), lastError: timerCheck.message });
      return NextResponse.json({
        ok: true,
        skipped: true,
        reason: "timer_expired",
        error: timerCheck.message,
      });
    }
    if (config.mode === "off" || !config.enabled) {
      await setState({ lastRunAt: Date.now(), lastError: undefined });
      return NextResponse.json({ ok: true, skipped: true, reason: "mode_off" });
    }
    if (config.mode === "live" && !PRIVATE_KEY) {
      return NextResponse.json({ error: "PRIVATE_KEY not configured for Live mode" }, { status: 500 });
    }
    let state = await getState();
    let dailyRisk = state.dailyRisk;

    if (config.mode === "live") {
      const liveCashBalance = await getCashBalance(MY_ADDRESS).catch(() => 0);
      dailyRisk = initDailyRiskState(state.dailyRisk, liveCashBalance);
      const dailyCheck = evaluateDailyRiskCaps({
        dailyRisk,
        cashBalance: liveCashBalance,
        maxDailyLiveNotionalUsd: config.maxDailyLiveNotionalUsd,
        maxDailyDrawdownUsd: config.maxDailyDrawdownUsd,
      });
      dailyRisk = dailyCheck.dailyRisk;
      if (dailyCheck.blocked) {
        const now = Date.now();
        await setState({
          lastRunAt: now,
          lastError: dailyCheck.message,
          dailyRisk,
        });
        if (dailyCheck.shouldAlert) {
          await sendAlert({
            title: `Live run blocked: ${dailyCheck.reason}`,
            severity: "critical",
            details: {
              drawdownUsd: dailyCheck.drawdownUsd,
              maxDailyLiveNotionalUsd: config.maxDailyLiveNotionalUsd,
              maxDailyDrawdownUsd: config.maxDailyDrawdownUsd,
              dailyRisk,
            },
          });
        }
        return NextResponse.json({
          ok: true,
          skipped: true,
          reason: dailyCheck.reason,
          error: dailyCheck.message,
        });
      }

      if (state.safetyLatch?.active) {
        const latchAttempt = await attemptResolveSafetyLatch({
          latch: state.safetyLatch,
          privateKey: PRIVATE_KEY ?? "",
          myAddress: MY_ADDRESS,
          signatureType: SIGNATURE_TYPE,
          unwindSellSlippageCents: config.unwindSellSlippageCents,
          unwindShareBufferPct: config.unwindShareBufferPct,
        });
        const now = Date.now();
        const shouldAlert = shouldSendLatchAlert(state.safetyLatch, now, LATCH_ALERT_COOLDOWN_MS);
        if (latchAttempt.resolved && latchAttempt.remainingAssets.length === 0) {
          if (shouldAlert) {
            await sendAlert({
              title: "Safety latch auto-cleared",
              severity: "info",
              details: {
                attemptedAssets: latchAttempt.attemptedAssets,
                resolvedAssets: latchAttempt.resolvedAssets,
                message: latchAttempt.message,
              },
            });
          }
          state = {
            ...state,
            safetyLatch: undefined,
            lastError: undefined,
            dailyRisk,
          };
        } else {
          const latchReason = latchAttempt.resolved
            ? `${latchAttempt.message}. Manual reset required to resume live runs.`
            : latchAttempt.message;
          const updatedLatch = {
            ...state.safetyLatch,
            active: true,
            reason: latchReason,
            unresolvedAssets: latchAttempt.remainingAssets,
            attempts: (state.safetyLatch.attempts ?? 0) + 1,
            lastAttemptAt: now,
            lastAlertAt: shouldAlert ? now : state.safetyLatch.lastAlertAt,
          };
          await setState({
            lastRunAt: now,
            lastError: latchReason,
            safetyLatch: updatedLatch,
            dailyRisk,
          });
          if (shouldAlert) {
            await sendAlert({
              title: "Safety latch active before live run",
              severity: latchAttempt.resolved ? "warning" : "critical",
              details: {
                attemptedAssets: latchAttempt.attemptedAssets,
                resolvedAssets: latchAttempt.resolvedAssets,
                failedAssets: latchAttempt.failedAssets,
                remainingAssets: latchAttempt.remainingAssets,
                attempts: updatedLatch.attempts,
                latchReason,
              },
            });
          }
          return NextResponse.json({
            ok: true,
            skipped: true,
            reason: "safety_latch_active",
            error: latchReason,
          });
        }
      }
    }

    const result = await runPairedStrategy(
      PRIVATE_KEY ?? "",
      MY_ADDRESS,
      SIGNATURE_TYPE,
      {
        mode: config.mode,
        walletUsagePercent: config.walletUsagePercent,
        pairChunkUsd: config.pairChunkUsd,
        maxRunBudgetUsd: config.maxRunBudgetUsd,
        paperVirtualWalletUsd: config.paperVirtualWalletUsd,
        capitalReservePercent: config.capitalReservePercent,
        minBetUsd: config.minBetUsd,
        stopLossBalance: config.stopLossBalance ?? 0,
        floorToPolymarketMin: config.floorToPolymarketMin !== false,
        pairMinEdgeCents: config.pairMinEdgeCents,
        paperAllowNegativeEdge: config.paperAllowNegativeEdge,
        paperMinEdgeCents: config.paperMinEdgeCents,
        pairMinEdgeCents5m: config.pairMinEdgeCents5m,
        pairMinEdgeCents15m: config.pairMinEdgeCents15m,
        pairMinEdgeCentsHourly: config.pairMinEdgeCentsHourly,
        pairFeeBps: config.pairFeeBps,
        pairSlippageCents: config.pairSlippageCents,
        liveMinNetEdgeSurplusCents: config.liveMinNetEdgeSurplusCents,
        adaptiveEdgeEnabled: config.adaptiveEdgeEnabled,
        adaptiveEdgeLowActivityTradeCount: config.adaptiveEdgeLowActivityTradeCount,
        adaptiveEdgeMaxPenaltyCents: config.adaptiveEdgeMaxPenaltyCents,
        adaptiveEdgeStalePenaltyCents: config.adaptiveEdgeStalePenaltyCents,
        adaptiveEdgeStalePenaltyCents5m: config.adaptiveEdgeStalePenaltyCents5m,
        adaptiveEdgeStalePenaltyCents15m: config.adaptiveEdgeStalePenaltyCents15m,
        adaptiveEdgeStalePenaltyCentsHourly: config.adaptiveEdgeStalePenaltyCentsHourly,
        freshnessMaxSignalAgeSec5m: config.freshnessMaxSignalAgeSec5m,
        freshnessMaxSignalAgeSec15m: config.freshnessMaxSignalAgeSec15m,
        freshnessMaxSignalAgeSecHourly: config.freshnessMaxSignalAgeSecHourly,
        freshnessMaxExecutionQuoteAgeSec5m: config.freshnessMaxExecutionQuoteAgeSec5m,
        freshnessMaxExecutionQuoteAgeSec15m: config.freshnessMaxExecutionQuoteAgeSec15m,
        freshnessMaxExecutionQuoteAgeSecHourly: config.freshnessMaxExecutionQuoteAgeSecHourly,
        paperRelaxFreshness: config.paperRelaxFreshness,
        paperFreshnessAgeMultiplier: config.paperFreshnessAgeMultiplier,
        dynamicSizingEnabled: config.dynamicSizingEnabled,
        dynamicSizingMinScalePct: config.dynamicSizingMinScalePct,
        dynamicSizingMaxScalePct: config.dynamicSizingMaxScalePct,
        dynamicSizingEdgeTargetCents: config.dynamicSizingEdgeTargetCents,
        dynamicSizingLiquidityTradeCount: config.dynamicSizingLiquidityTradeCount,
        edgeBoostEnabled: config.edgeBoostEnabled,
        edgeBoostThresholdCents: config.edgeBoostThresholdCents,
        edgeBoostHighThresholdCents: config.edgeBoostHighThresholdCents,
        edgeBoostScalePct: config.edgeBoostScalePct,
        edgeBoostHighScalePct: config.edgeBoostHighScalePct,
        pairLookbackSeconds: config.pairLookbackSeconds,
        pairMaxMarketsPerRun: config.pairMaxMarketsPerRun,
        reentryMaxEntriesPerSignal: config.reentryMaxEntriesPerSignal,
        reentryEdgeStepCents: config.reentryEdgeStepCents,
        maxConditionExposureUsd: config.maxConditionExposureUsd,
        maxCoinExposureSharePct: config.maxCoinExposureSharePct,
        maxCadenceExposureSharePct: config.maxCadenceExposureSharePct,
        autoExitResidualPositions: config.autoExitResidualPositions,
        residualPositionMinUsd: config.residualPositionMinUsd,
        residualPositionMaxPerRun: config.residualPositionMaxPerRun,
        residualPositionSellDiscountCents: config.residualPositionSellDiscountCents,
        enableBtc: config.enableBtc,
        enableEth: config.enableEth,
        enableCadence5m: config.enableCadence5m,
        enableCadence15m: config.enableCadence15m,
        enableCadenceHourly: config.enableCadenceHourly,
        maxUnresolvedImbalancesPerRun: config.maxUnresolvedImbalancesPerRun,
        unwindSellSlippageCents: config.unwindSellSlippageCents,
        unwindShareBufferPct: config.unwindShareBufferPct,
      },
      { lastTimestamp: state.lastTimestamp, copiedKeys: state.copiedKeys }
    );

    const diagnostics = {
      mode: result.mode,
      evaluatedSignals: result.evaluatedSignals,
      eligibleSignals: result.eligibleSignals,
      rejectedReasons: result.rejectedReasons,
      evaluatedBreakdown: result.evaluatedBreakdown,
      eligibleBreakdown: result.eligibleBreakdown,
      executedBreakdown: result.executedBreakdown,
      copied: result.copied,
      paper: result.paper,
      failed: result.failed,
      budgetCapUsd: result.budgetCapUsd,
      budgetUsedUsd: result.budgetUsedUsd,
      avgExecutedEdgeCents: result.avgExecutedEdgeCents,
      avgExecutedNetEdgeCents: result.avgExecutedNetEdgeCents,
      error: result.error,
      timestamp: Date.now(),
      maxEdgeCentsSeen: result._maxEdgeCents,
      maxNetEdgeCentsSeen: result._maxNetEdgeCents,
      minPairSumSeen: result._minPairSum,
    };

    const now = Date.now();
    let safetyLatch = state.safetyLatch;
    if (
      config.mode === "live" &&
      (result.rejectedReasons["circuit_breaker_unresolved_imbalance"] ?? 0) > 0
    ) {
      safetyLatch = {
        active: true,
        reason: result.error ?? "Circuit breaker tripped due to unresolved imbalance",
        triggeredAt: now,
        unresolvedAssets: result.unresolvedExposureAssets,
        attempts: 0,
      };
      await sendAlert({
        title: "Live circuit breaker tripped",
        severity: "critical",
        details: {
          unresolvedAssets: result.unresolvedExposureAssets,
          rejectedReasons: result.rejectedReasons,
          error: result.error,
        },
      });
    }

    if (config.mode === "live" && result.failed >= 3) {
      await sendAlert({
        title: "Live run had repeated failed orders",
        severity: "warning",
        details: {
          failed: result.failed,
          copied: result.copied,
          error: result.error,
          rejectedReasons: result.rejectedReasons,
        },
      });
    }

    const nextDailyRisk =
      config.mode === "live"
        ? applyDailyLiveRun(
            dailyRisk ?? initDailyRiskState(state.dailyRisk, await getCashBalance(MY_ADDRESS).catch(() => 0), now),
            result.budgetUsedUsd,
            now
          )
        : state.dailyRisk;

    await setState({
      lastTimestamp: result.lastTimestamp ?? state.lastTimestamp,
      copiedKeys: result.copiedKeys.length > 0 ? result.copiedKeys : state.copiedKeys,
      lastRunAt: now,
      lastCopiedAt: result.copied > 0 ? now : state.lastCopiedAt,
      lastError: result.error,
      lastStrategyDiagnostics: diagnostics,
      dailyRisk: nextDailyRisk,
      safetyLatch,
    });
    await recordStrategyDiagnostics(diagnostics);
    if (result.copiedTrades?.length) {
      await appendActivity(result.copiedTrades);
      if (result.mode === "paper") {
        await recordPaperTrades(result.copiedTrades);
      }
    }
    if (result.mode === "paper") {
      await recordPaperRun({
        timestamp: Date.now(),
        simulatedTrades: result.paper,
        simulatedVolumeUsd: result.simulatedVolumeUsd,
        failed: result.failed,
        budgetCapUsd: result.budgetCapUsd,
        budgetUsedUsd: result.budgetUsedUsd,
        executedEdgeCentsSum: result.executedEdgeCentsSum,
        executedNetEdgeCentsSum: result.executedNetEdgeCentsSum,
        avgExecutedEdgeCents: result.avgExecutedEdgeCents,
        avgExecutedNetEdgeCents: result.avgExecutedNetEdgeCents,
        error: result.error,
      });
    }

    return NextResponse.json({
      ok: true,
      mode: result.mode,
      copied: result.copied,
      paper: result.paper,
      simulatedVolumeUsd: result.simulatedVolumeUsd,
      failed: result.failed,
      evaluatedSignals: result.evaluatedSignals,
      eligibleSignals: result.eligibleSignals,
      rejectedReasons: result.rejectedReasons,
      budgetCapUsd: result.budgetCapUsd,
      budgetUsedUsd: result.budgetUsedUsd,
      avgExecutedEdgeCents: result.avgExecutedEdgeCents,
      avgExecutedNetEdgeCents: result.avgExecutedNetEdgeCents,
      error: result.error,
    });
  } catch (e) {
    const err = e instanceof Error ? e.message : String(e);
    console.error("Strategy run error:", e);
    return NextResponse.json({ ok: false, error: err }, { status: 500 });
  } finally {
    await releaseRunLock(lockToken).catch((e) => {
      console.error("Failed releasing run lock:", e);
    });
  }
}
